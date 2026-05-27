-- Witness.lua — наблюдаемый код.
--
-- ┌──────────────────────────────────────────────────────────────────────────┐
-- │ Witness — это твоя память. Код пишет события, ты задаёшь вопросы.        │
-- │                                                                          │
-- │ Не printf-debugging. Не stack trace. Не logger.                          │
-- │                                                                          │
-- │ Код декларирует "наблюдай за этой областью". Witness записывает          │
-- │ упорядоченный поток событий: вызовы функций, изменения свойств,          │
-- │ создание/уничтожение инстансов, ошибки, пользовательские маркеры.        │
-- │                                                                          │
-- │ Когда нужно — спрашиваешь:                                               │
-- │   W.recent(5)                — что было за 5 секунд                      │
-- │   W.history(instance)        — вся жизнь этого инстанса                  │
-- │   W.why_nil(inst, "Parent")  — когда и кто сделал nil                    │
-- │   W.last_error()             — последняя ошибка с контекстом             │
-- │   W.trace_scope("c00lkidd")  — события в именованной области             │
-- │   W.dump()                   — всё, как текст, для отправки разработчику │
-- │                                                                          │
-- │ ВКЛЮЧЕНИЕ/ВЫКЛЮЧЕНИЕ:                                                    │
-- │   Witness OFF (default) → ноль allocations, ноль работы.                 │
-- │   Witness ON  → ring buffer константного размера, пишет только в         │
-- │                  наблюдаемых областях.                                   │
-- └──────────────────────────────────────────────────────────────────────────┘

local Witness = {}

-- ── глобальное состояние ──────────────────────────────────────────────────

-- Ring buffer событий. Константный размер. Старые перезаписываются.
local BUFFER_SIZE = 2048
local buffer = table.create and table.create(BUFFER_SIZE) or {}
local buffer_head = 0       -- индекс куда писать следующее событие (mod BUFFER_SIZE)
local buffer_count = 0      -- сколько событий уже записано (для отображения T+x)

-- Включён ли Witness вообще
local enabled = false

-- Активные scopes-наблюдатели. Каждый — {name, started_at, until_t?}.
-- При записи события — Witness смотрит "есть ли активный scope в стеке?",
-- и если нет — пропускает (даже если enabled=true). Это позволяет включить
-- Witness на запуске, но фактически записывать только когда явно запрошено.
local active_scopes = {}      -- stack
local started_at_global = nil -- момент включения Witness (для T+x относительно)

-- Инстанс-tracking: каждый инстанс который мы видели — получает id.
-- Это позволяет позже спросить "что было с инстансом X" даже после его Destroy.
local instance_to_id = setmetatable({}, {__mode = "k"})
local id_to_meta = {}   -- [id] = {class, name, born_at, died_at?, last_seen_t}
local next_id = 1

-- Property watchers: [instance] = {[propname] = connection}
-- Чтобы не делать GetPropertyChangedSignal дважды для одного и того же.
local prop_watchers = setmetatable({}, {__mode = "k"})

-- ── time ──────────────────────────────────────────────────────────────────

local _clock = (os and os.clock) or function() return tick and tick() or 0 end
local function now() return _clock() end

-- ── instance ids ──────────────────────────────────────────────────────────

local function instance_id(inst)
    if not inst then return nil end
    local id = instance_to_id[inst]
    if id then return id end
    id = next_id
    next_id = next_id + 1
    instance_to_id[inst] = id
    local class = "?"
    local name = "?"
    pcall(function() class = inst.ClassName end)
    pcall(function() name = inst.Name end)
    id_to_meta[id] = {
        class = class,
        name = name,
        born_at = now(),
        last_seen_t = now(),
    }
    return id
end

-- ── value shortening (для записи в события) ────────────────────────────────
-- Не записываем целые таблицы. Только тип + краткий repr.

local function short(v, depth)
    depth = depth or 0
    local t = type(v)
    if v == nil then return "nil" end
    if t == "boolean" then return tostring(v) end
    if t == "number" then
        if v ~= v then return "nan" end
        if v == math.huge then return "inf" end
        if v == -math.huge then return "-inf" end
        return tostring(v)
    end
    if t == "string" then
        if #v > 40 then return ('"%s..."'):format(v:sub(1, 37)) end
        return ('"%s"'):format(v)
    end
    if t == "table" then
        if depth > 1 then return "{...}" end
        local n = 0
        for _ in pairs(v) do n = n + 1; if n > 3 then break end end
        return ("{n=%d}"):format(n)
    end
    if t == "function" then return "<fn>" end
    if t == "thread" then return "<thread>" end
    if t == "userdata" then
        -- Roblox типы: Vector3, CFrame, Instance, etc
        if typeof then
            local rt = typeof(v)
            if rt == "Instance" then
                local id = instance_id(v)
                local meta = id_to_meta[id]
                return ("[%s:%s#%d]"):format(meta.class, meta.name, id)
            end
            if rt == "Vector3" then
                return ("V3(%.1f,%.1f,%.1f)"):format(v.X, v.Y, v.Z)
            end
            if rt == "CFrame" then
                return ("CFr(%.1f,%.1f,%.1f)"):format(v.Position.X, v.Position.Y, v.Position.Z)
            end
            return ("<%s>"):format(rt)
        end
        return "<userdata>"
    end
    return tostring(v)
end

local function shorten_args(args, n)
    local parts = {}
    for i = 1, math.min(n, 4) do
        parts[i] = short(args[i])
    end
    if n > 4 then parts[#parts + 1] = ("...+%d"):format(n - 4) end
    return table.concat(parts, ", ")
end

-- ── event recording ───────────────────────────────────────────────────────
-- Каждое событие: {t, kind, scope, ...payload}.
-- Поля payload зависят от kind. Чтобы избежать allocations на hot path,
-- мы реиспользуем slot в ring buffer (overwrite).

local function record(kind, payload)
    if not enabled then return end
    if #active_scopes == 0 then return end

    buffer_head = (buffer_head % BUFFER_SIZE) + 1
    buffer_count = buffer_count + 1

    local slot = buffer[buffer_head]
    if not slot then
        slot = {}
        buffer[buffer_head] = slot
    end

    slot.t = now()
    slot.kind = kind
    slot.scope = active_scopes[#active_scopes].name
    slot.seq = buffer_count

    -- copy payload into slot — без allocation новой таблицы
    -- сначала чистим старые поля slot'а кроме служебных
    for k in pairs(slot) do
        if k ~= "t" and k ~= "kind" and k ~= "scope" and k ~= "seq" then
            slot[k] = nil
        end
    end
    if payload then
        for k, v in pairs(payload) do slot[k] = v end
    end
end

-- ── public: scope management ──────────────────────────────────────────────

-- Открывает новую область наблюдения. Возвращает функцию-закрывалку.
-- Использование:
--   local close = W.observe("c00lkidd_spawn")
--   ... код ...
--   close()
function Witness.observe(name)
    if not enabled then
        return function() end
    end
    if not started_at_global then started_at_global = now() end
    local scope = {name = name, started_at = now()}
    active_scopes[#active_scopes + 1] = scope
    record("scope_open", {name = name})

    return function()
        record("scope_close", {name = name})
        -- pop scope. Если стек повреждён (close из другого порядка) — predict and recover
        for i = #active_scopes, 1, -1 do
            if active_scopes[i] == scope then
                table.remove(active_scopes, i)
                break
            end
        end
    end
end

-- Альтернатива: with-style. Тело-функция, скоуп открывается и закрывается автоматически.
--   W.with("c00lkidd_spawn", function() ... end)
function Witness.with(name, body)
    local close = Witness.observe(name)
    local ok, err = pcall(body)
    close()
    if not ok then error(err, 0) end
end

-- ── public: enabling ──────────────────────────────────────────────────────

function Witness.enable()
    enabled = true
    if not started_at_global then started_at_global = now() end
end

function Witness.disable()
    enabled = false
    -- активные prop_watchers не убиваем — они автоматически no-op в записи
    -- через `enabled` check.
end

function Witness.is_enabled() return enabled end

-- ── public: function wrapping ─────────────────────────────────────────────
--
-- W.observed(name, fn) — оборачивает функцию, записывая call/return/error.
-- Когда Witness выключен или нет активного scope — оверхед один if. Цена приемлема.

function Witness.observed(name, fn)
    return function(...)
        if not enabled or #active_scopes == 0 then
            return fn(...)
        end
        local args = {...}
        local n = select("#", ...)
        record("call", {fn = name, args = shorten_args(args, n), nargs = n})

        local results = table.pack(pcall(fn, ...))
        local ok = results[1]

        if ok then
            local rn = results.n - 1
            local rargs = {}
            for i = 2, results.n do rargs[i - 1] = results[i] end
            record("return", {fn = name, ret = shorten_args(rargs, rn), nret = rn})
            return table.unpack(results, 2, results.n)
        else
            local err = results[2]
            record("error", {fn = name, err = tostring(err)})
            error(err, 0)  -- ре-пробрасываем как в pcall'е
        end
    end
end

-- W.wrap_table(tbl, prefix) — обернёт все function-поля таблицы.
-- prefix используется как префикс к имени в логе ("C00lkidd.spawnJaneGhost").
function Witness.wrap_table(tbl, prefix)
    for k, v in pairs(tbl) do
        if type(v) == "function" then
            local name = (prefix and (prefix .. "." .. tostring(k))) or tostring(k)
            tbl[k] = Witness.observed(name, v)
        end
    end
    return tbl
end

-- ── public: user marks ────────────────────────────────────────────────────

function Witness.mark(label, data)
    record("mark", {label = label, data = data and short(data) or nil})
end

-- ── public: instance lifecycle ────────────────────────────────────────────
-- Когда тебя интересует жизнь конкретного инстанса — track его.

function Witness.track(instance)
    if not enabled or not instance then return instance end
    local id = instance_id(instance)
    local meta = id_to_meta[id]
    record("track_begin", {id = id, class = meta.class, name = meta.name})

    -- следим за Destroying
    local ok, conn = pcall(function()
        return instance.Destroying:Connect(function()
            local m = id_to_meta[id]
            if m then m.died_at = now() end
            record("destroyed", {id = id, class = m and m.class, name = m and m.name})
        end)
    end)
    -- conn не сохраняем — Destroying однократный, само отключится

    return instance
end

-- Witness.watch_prop(instance, prop_name) — пишет prop_set каждый раз
-- когда свойство меняется.
function Witness.watch_prop(instance, prop_name)
    if not enabled or not instance then return end
    local watchers = prop_watchers[instance]
    if not watchers then
        watchers = {}
        prop_watchers[instance] = watchers
    end
    if watchers[prop_name] then return end  -- уже подписан

    local id = instance_id(instance)
    local ok, conn = pcall(function()
        return instance:GetPropertyChangedSignal(prop_name):Connect(function()
            local new_val
            pcall(function() new_val = instance[prop_name] end)
            record("prop_set", {
                id = id, prop = prop_name, val = short(new_val),
            })
        end)
    end)
    if ok and conn then watchers[prop_name] = conn end
end

-- ── public: queries ───────────────────────────────────────────────────────
-- "Что было?". Возвращают list событий из buffer'а, отфильтрованных.

local function _iterate_buffer(filter)
    local result = {}
    -- buffer как кольцо: события в порядке seq. Идём от старого к новому.
    -- Старое — это (buffer_head + 1) если буфер полный, иначе 1.
    local start, count
    if buffer_count >= BUFFER_SIZE then
        start = buffer_head + 1
        count = BUFFER_SIZE
    else
        start = 1
        count = buffer_count
    end
    for i = 0, count - 1 do
        local idx = ((start - 1 + i) % BUFFER_SIZE) + 1
        local ev = buffer[idx]
        if ev and (not filter or filter(ev)) then
            result[#result + 1] = ev
        end
    end
    return result
end

function Witness.recent(seconds)
    local cutoff = now() - (seconds or 5)
    return _iterate_buffer(function(ev) return ev.t >= cutoff end)
end

function Witness.history(instance)
    if not instance then return {} end
    local id = instance_to_id[instance]
    if not id then return {} end
    return _iterate_buffer(function(ev) return ev.id == id end)
end

function Witness.trace_scope(scope_name)
    return _iterate_buffer(function(ev) return ev.scope == scope_name end)
end

function Witness.errors()
    return _iterate_buffer(function(ev) return ev.kind == "error" end)
end

function Witness.last_error()
    local errs = Witness.errors()
    return errs[#errs]
end

-- Causality: когда и почему свойство стало nil/изменилось на конкретное значение
function Witness.why_changed(instance, prop_name)
    if not instance then return nil end
    local id = instance_to_id[instance]
    if not id then return nil end
    local events = _iterate_buffer(function(ev)
        return ev.kind == "prop_set" and ev.id == id and ev.prop == prop_name
    end)
    return events
end

-- ── public: formatting ────────────────────────────────────────────────────
-- Превращает events в читаемый текст.

local function format_event(ev, t_zero)
    local t_rel = ev.t - t_zero
    local prefix = ("T+%.3f [%s] %s"):format(t_rel, ev.scope or "?", ev.kind)
    if ev.kind == "call" then
        return ("%s  %s(%s)"):format(prefix, ev.fn or "?", ev.args or "")
    elseif ev.kind == "return" then
        return ("%s  %s → %s"):format(prefix, ev.fn or "?", ev.ret or "")
    elseif ev.kind == "error" then
        return ("%s  %s ✗ %s"):format(prefix, ev.fn or "?", ev.err or "?")
    elseif ev.kind == "prop_set" then
        local meta = id_to_meta[ev.id]
        local who = meta and ("[%s:%s#%d]"):format(meta.class, meta.name, ev.id) or "?"
        return ("%s  %s.%s = %s"):format(prefix, who, ev.prop or "?", ev.val or "?")
    elseif ev.kind == "mark" then
        return ("%s  ★ %s%s"):format(prefix, ev.label or "?", ev.data and (" : " .. ev.data) or "")
    elseif ev.kind == "scope_open" then
        return ("%s  ┌ scope '%s' opened"):format(prefix, ev.name or "?")
    elseif ev.kind == "scope_close" then
        return ("%s  └ scope '%s' closed"):format(prefix, ev.name or "?")
    elseif ev.kind == "track_begin" then
        return ("%s  ● tracking [%s:%s#%d]"):format(prefix, ev.class or "?", ev.name or "?", ev.id or 0)
    elseif ev.kind == "destroyed" then
        return ("%s  ✗ destroyed [%s:%s#%d]"):format(prefix, ev.class or "?", ev.name or "?", ev.id or 0)
    end
    return prefix
end

function Witness.format(events)
    if not events or #events == 0 then return "(empty)" end
    local t_zero = events[1].t
    local lines = {}
    for i, ev in ipairs(events) do
        lines[i] = format_event(ev, t_zero)
    end
    return table.concat(lines, "\n")
end

function Witness.dump()
    return Witness.format(_iterate_buffer())
end

-- Печать в stdout. Для удобства в Roblox-консоли.
function Witness.print_recent(seconds)
    print(Witness.format(Witness.recent(seconds)))
end

function Witness.print_history(instance)
    print(Witness.format(Witness.history(instance)))
end

function Witness.print_dump()
    print(Witness.dump())
end

-- ── public: stats ─────────────────────────────────────────────────────────

function Witness.stats()
    local kinds = {}
    local scopes = {}
    for i = 1, math.min(buffer_count, BUFFER_SIZE) do
        local ev = buffer[i]
        if ev then
            kinds[ev.kind] = (kinds[ev.kind] or 0) + 1
            if ev.scope then scopes[ev.scope] = (scopes[ev.scope] or 0) + 1 end
        end
    end
    return {
        enabled = enabled,
        total_events = buffer_count,
        in_buffer = math.min(buffer_count, BUFFER_SIZE),
        buffer_size = BUFFER_SIZE,
        tracked_instances = next_id - 1,
        active_scopes = #active_scopes,
        kinds = kinds,
        scopes = scopes,
    }
end

-- Полный reset — для тестов или новой сессии
function Witness.reset()
    buffer = table.create and table.create(BUFFER_SIZE) or {}
    buffer_head = 0
    buffer_count = 0
    active_scopes = {}
    started_at_global = nil
    instance_to_id = setmetatable({}, {__mode = "k"})
    id_to_meta = {}
    next_id = 1
    -- prop_watchers не очищаем — они с weak keys, GC уберёт
end

return Witness
