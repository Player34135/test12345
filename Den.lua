-- Den.lua — lifecycle-as-place.
--
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  В Scope ты говорил: "вот объект, добавь его в эту коллекцию".          │
-- │  В Den ты говоришь: "выполни этот код вот ЗДЕСЬ". Всё что родилось      │
-- │  во время исполнения — родилось ЗДЕСЬ и умрёт когда ЗДЕСЬ закроется.   │
-- │                                                                         │
-- │  Den — это место. Не контейнер.                                         │
-- └─────────────────────────────────────────────────────────────────────────┘
--
-- API в 30 секунд:
--
--   Den "killer" / function()
--       on(humanoid.Died, function() ... end)        -- авто-Disconnect
--       after(2, function() ... end)                 -- авто-cancel
--       every(0.1, function() ... end)               -- авто-cancel
--       on_step(function(dt) ... end)                -- авто-Unbind
--       own(Instance.new("Sound", workspace))        -- авто-Destroy
--
--       Den "ghost" / function()                     -- ребёнок-нора
--           bind_to(janeModel)                       -- умрёт с jane
--           on(...)
--       end
--
--       until_(jane.Destroying)                      -- блокирует, потом тело завершается
--   end                                              -- ← всё внутри умерло
--
-- Реактивщина:
--
--   local killer = signal(nil)
--   killer:set(c00lkidd)
--   killer:get()
--
--   reactive(function()
--       local k = killer:read()                      -- read() = подписка
--       if not k then return end
--       Den "active_killer" / function()             -- эта нора создаётся
--           on(k.Died, function() killer:set(nil) end)
--           -- ...
--       end                                          -- ← и умирает когда killer изменится
--   end)
--
-- Защита от утечек:
--   * on/after/every/own/bind_to ВНЕ Den — runtime error.
--   * Den-тело это функция, не таблица. Выход = смерть. Раннер не нужен.
--   * Дети умирают раньше родителя. Внутри одной норы — LIFO.
--   * Двойной Close — no-op. Close из callback'а изнутри — отложен до конца кадра.
--
-- НИЧЕГО кроме этого файла. Никаких track(), Add(), Cleanup(), :Child(),
-- :Destroy()-recreate. Если ты пишешь Destroy явно — ты делаешь что-то не так.

local Den = {}
Den.__index = Den

-- ── ambient stack ────────────────────────────────────────────────────────────
-- Стек "где я сейчас". Один на coroutine. Доступ через _current().
-- Когда Den открывается — push. Когда тело возвращается — pop.

local _stack_key = setmetatable({}, {__tostring = function() return "den.stack" end})
local _stacks = setmetatable({}, {__mode = "k"})   -- coroutine → stack

local function _stack()
    local co = coroutine.running()
    local s = _stacks[co]
    if not s then s = {}; _stacks[co] = s end
    return s
end

local function _current()
    local s = _stack()
    return s[#s]
end

local function _require_current(op)
    local cur = _current()
    if not cur then
        error(("Den: %s() called outside any Den. Where would it live?\n"
            .. "Wrap your code in `Den 'name' / function() ... end` first."):format(op), 3)
    end
    if cur._dead or cur._dying then
        error(("Den: %s() called inside a dying Den '%s'."):format(op, cur._name or "?"), 3)
    end
    return cur
end

-- ── teardown primitives (типы того что мы умеем убирать) ────────────────────

local function _kill(obj, den_name)
    local t = typeof(obj)
    if t == "RBXScriptConnection" then
        pcall(obj.Disconnect, obj)
    elseif t == "Instance" then
        pcall(obj.Destroy, obj)
    elseif type(obj) == "function" then
        local ok, err = pcall(obj)
        if not ok then warn(("[Den '%s'] teardown error: %s"):format(den_name or "?", err)) end
    elseif type(obj) == "thread" then
        pcall(task.cancel, obj)
    end
end

-- ── Den core ─────────────────────────────────────────────────────────────────

local function _new_den(name, parent)
    return setmetatable({
        _name      = name,
        _parent    = parent,
        _tasks     = {},      -- LIFO список того что надо убрать
        _children  = {},      -- {[child] = idx_in_parent_children_list}; для O(1) удаления
        _children_n = 0,
        _dead      = false,
        _dying     = false,
        _on_close  = nil,     -- список однократных callback'ов "когда я закроюсь"
        _pidx      = nil,     -- индекс этой норы в parent._children_list (для O(1) разрыва)
        _clist     = nil,     -- parent._children_list cached
    }, Den)
end

-- Внутренний регистратор задачи. Все public-функции (on/own/after/...) идут через это.
function Den:_track(obj)
    if self._dead or self._dying then
        _kill(obj, self._name)
        return obj
    end
    self._tasks[#self._tasks + 1] = obj
    return obj
end

-- Дочерняя нора. Регистрируется в родителе O(1)-swap-delete'ом.
function Den:_child(name)
    local child = _new_den(name, self)
    if not self._children_list then self._children_list = {} end
    self._children_list[#self._children_list + 1] = child
    child._pidx = #self._children_list
    child._clist = self._children_list
    return child
end

-- Близость смерти.
function Den:is_alive()
    return not (self._dead or self._dying)
end

-- Принудительное закрытие. Обычно не нужно — нора закрывается сама когда её тело
-- возвращается. Но иногда (reactive recompute, явный stop) — надо.
function Den:close()
    if self._dead or self._dying then return end
    self._dying = true

    -- on_close callbacks (для reactive — снять подписки)
    if self._on_close then
        for i = #self._on_close, 1, -1 do
            local ok, err = pcall(self._on_close[i])
            if not ok then warn(("[Den '%s'] on_close error: %s"):format(self._name or "?", err)) end
        end
        self._on_close = nil
    end

    -- отвязка от родителя O(1)
    local clist = self._clist
    if clist then
        local idx, last = self._pidx, #clist
        if idx and idx <= last then
            if idx ~= last then
                local moved = clist[last]
                clist[idx] = moved
                moved._pidx = idx
            end
            clist[last] = nil
        end
        self._clist = nil
        self._pidx = nil
    end

    -- дети первыми, в LIFO порядке
    local kids = self._children_list
    if kids then
        for i = #kids, 1, -1 do
            local c = kids[i]
            if c and not c._dead then
                c._clist = nil  -- защита от попытки удалить себя из уже-снимаемого parent'а
                c._pidx = nil
                c:close()
            end
        end
        self._children_list = nil
    end

    -- свои задачи, LIFO
    local tasks = self._tasks
    self._tasks = nil
    for i = #tasks, 1, -1 do
        _kill(tasks[i], self._name)
    end

    self._dying = false
    self._dead = true
end

-- Хук "позови меня когда я буду закрыта". Используется reactive(). Не путать с
-- own(function) — on_close дешевле (не идёт в _tasks, а в отдельный список) и
-- гарантированно бежит ДО tasks/детей.
function Den:_on_close_cb(fn)
    if self._dead then pcall(fn); return end
    if not self._on_close then self._on_close = {} end
    self._on_close[#self._on_close + 1] = fn
end

-- ── публичная фабрика: Den "name" / function() ... end ──────────────────────
-- Den — callable таблица. Den "x" возвращает builder, builder / fn открывает нору
-- и сразу исполняет тело внутри неё.
--
-- Парсинг:
--   Den "killer"            → builder с name="killer", parent=auto
--   Den "killer" / fn       → запуск
--   Den.root "killer" / fn  → forced-root (без родителя)
--   builder:as_child_of(d)  → явный родитель (редко нужно — auto работает)

local _Builder = {}
_Builder.__index = _Builder

local function _make_builder(name, force_parent, forced_root)
    return setmetatable({
        _name = name,
        _forced_parent = force_parent,
        _forced_root = forced_root,
    }, _Builder)
end

function _Builder:as_child_of(parent)
    self._forced_parent = parent
    self._forced_root = false
    return self
end

-- den_builder(function() ... end) — главный синтаксис.
--
-- Изначально хотелось `Den "x" / function() ... end` через __div, но Lua-парсер
-- не принимает бинарное выражение как statement (только function-call или
-- assignment). А `Den "x" (function() ... end)` — это уже chained function-call
-- (string-call + table-call), и парсер счастлив. Бонус: визуально читается даже
-- лучше — тело выглядит как аргумент места, чем оно и является.
function _Builder:__call(body)
    local parent
    if self._forced_root then
        parent = nil
    elseif self._forced_parent then
        parent = self._forced_parent
    else
        parent = _current()  -- ambient
    end

    local den
    if parent then
        den = parent:_child(self._name)
    else
        den = _new_den(self._name, nil)
    end

    -- push, run, pop. При ошибке тело — нора закрывается, ошибка перебрасывается.
    local s = _stack()
    s[#s + 1] = den
    local ok, err = pcall(body)
    s[#s] = nil

    if not ok then
        den:close()
        error(err, 0)
    end

    -- Тело вернулось. Нора живёт до явного close() или до смерти родителя.
    -- Это намеренно: декларативный setup обычно не "закрывается сам".
    -- Для "после ожидания закройся" — return Den.close_self() после until_().
    return den
end

-- Сам Den — callable, чтобы можно было `Den "name"`.
setmetatable(Den, {
    __call = function(_, name)
        return _make_builder(name)
    end,
})

-- Den.root "name" / fn — для самой первой норы (root) когда ambient ещё пуст.
Den.root = setmetatable({}, {
    __call = function(_, name)
        return _make_builder(name, nil, true)
    end,
})

-- ── ambient API: тут живут все track-операции ──────────────────────────────
--
-- Эти функции — НЕ методы. Они вообще не знают про конкретный Den. Они спрашивают
-- "где я сейчас" и регистрируются ТАМ. Это и есть ambient — никаких "scope:Add".

local function on(signal, fn)
    local d = _require_current("on")
    return d:_track(signal:Connect(fn))
end

local function once(signal, fn)
    local d = _require_current("once")
    local handle = {conn = nil}
    handle.conn = signal:Connect(function(...)
        local c = handle.conn
        if c then c:Disconnect(); handle.conn = nil end
        if d._dead or d._dying then return end
        fn(...)
    end)
    return d:_track(handle.conn)
end

local function after(seconds, fn)
    local d = _require_current("after")
    local thread = task.delay(seconds, function()
        if d._dead or d._dying then return end
        fn()
    end)
    return d:_track(thread)
end

local function defer(fn)
    local d = _require_current("defer")
    local thread = task.defer(function()
        if d._dead or d._dying then return end
        fn()
    end)
    return d:_track(thread)
end

-- every: использует флаг alive вместо task.cancel (последний не работает на
-- активной coroutine, только на yielded — известный pitfall).
local function every(interval, fn)
    local d = _require_current("every")
    local alive = true
    task.spawn(function()
        while alive and d:is_alive() do
            task.wait(interval)
            if alive and d:is_alive() then
                local ok, err = pcall(fn)
                if not ok then warn(("[Den '%s'] every() error: %s"):format(d._name, err)) end
            end
        end
    end)
    d:_track(function() alive = false end)
    return function() alive = false end
end

-- on_step / on_heartbeat / on_pre_sim — авто-Unbind через cleanup-фн.
local _RunService = game:GetService("RunService")

local function on_step(name, priority, fn)
    -- 2-аргументный вариант: on_step(fn) → имя авто, приоритет=Camera+1
    if type(name) == "function" then
        fn = name
        name = "DenStep_" .. tostring(math.random(1e9))
        priority = Enum.RenderPriority.Camera.Value + 1
    end
    local d = _require_current("on_step")
    _RunService:BindToRenderStep(name, priority, fn)
    d:_track(function() pcall(_RunService.UnbindFromRenderStep, _RunService, name) end)
end

local function on_heartbeat(fn)
    local d = _require_current("on_heartbeat")
    return d:_track(_RunService.Heartbeat:Connect(fn))
end

local function on_pre_sim(fn)
    local d = _require_current("on_pre_sim")
    return d:_track(_RunService.PreSimulation:Connect(fn))
end

-- own(): что угодно "посади тут жить". Instance, Connection, function, thread.
local function own(obj)
    local d = _require_current("own")
    return d:_track(obj)
end

-- bind_to: эта нора умирает когда инстанс уничтожен. Read: "посади эту нору ВНУТРИ
-- этого инстанса в игровом дереве". Идиоматичная замена `:BindToInstance(jane)`.
local function bind_to(instance)
    local d = _require_current("bind_to")
    return d:_track(instance.Destroying:Connect(function() d:close() end))
end

-- until_: блокирует тело текущей норы, ждёт сигнал, потом возвращается.
-- Идиоматично: `Den "..." / function() ...setup...; until_(jane.Destroying) end`
-- — после until_ тело вернулось, но нора жива пока её явно не закроют или пока
-- родитель не умрёт. Если хочешь "после ожидания закрыть себя" — return close_self().
local function until_(signal)
    local d = _require_current("until")
    if not d:is_alive() then return end
    local thread = coroutine.running()
    local fired = false
    local conn
    conn = signal:Connect(function()
        if fired then return end
        fired = true
        if conn then conn:Disconnect() end
        if coroutine.status(thread) == "suspended" then
            task.spawn(thread)
        end
    end)
    d:_track(conn)
    -- если нора умрёт раньше сигнала — конн отключится в teardown,
    -- но мы тут так и зависнем. Защита: own cleanup-фн которая разбудит нас.
    d:_track(function()
        if not fired and coroutine.status(thread) == "suspended" then
            fired = true
            task.spawn(thread)
        end
    end)
    coroutine.yield()
end

-- close_self: ярлык "закрой нору в которой я сейчас".
local function close_self()
    local d = _current()
    if d then d:close() end
end

-- inside: получить ссылку на текущую нору (для редких случаев — например ручное
-- управление дочерним фабричным кодом). 99% кода эту функцию никогда не позовёт.
local function inside()
    return _current()
end

-- ── reactive: signals + effects с авто-инвалидацией норы ────────────────────
--
-- signal(initial) → объект с :get(), :set(v), :read() (=:get + подписаться-если-в-reactive)
-- reactive(fn)    → запускает fn внутри tracking-режима. Все signal:read() во время fn
--                   автоматически подпишут текущий reactive. При :set() — fn перезапускается,
--                   ПРЕДЫДУЩАЯ нора закрывается, новая открывается. Магия.

local Signal = {}
Signal.__index = Signal

local _current_reactive = nil  -- {add_dep = function(signal_obj)}

function Signal:get() return self._v end

function Signal:set(v)
    if rawequal(self._v, v) then return end
    self._v = v
    -- копия subs — подписчик может перерегистрироваться во время notify
    local subs = self._subs
    if not subs or not next(subs) then return end
    local copy = {}
    for s in pairs(subs) do copy[#copy + 1] = s end
    for i = 1, #copy do
        local fn = copy[i]
        if subs[fn] then  -- ещё подписан
            pcall(fn)
        end
    end
end

function Signal:read()
    if _current_reactive then
        _current_reactive(self)
    end
    return self._v
end

local function signal(initial)
    return setmetatable({_v = initial, _subs = setmetatable({}, {__mode = "k"})}, Signal)
end

-- reactive(fn): запускает fn в режиме отслеживания. fn создаёт нору (обычно через
-- Den "..." / function() ... end). При изменении любого signal:read() из fn — нора
-- закрывается и fn зовётся снова. Без слов "destroy + recreate" нигде в твоём коде.
local function reactive(fn)
    local d = _require_current("reactive")
    local active = true
    local current_inner_den = nil
    local deps = {}  -- [signal] = true — текущие зависимости

    -- forward declared (notify ссылается в clear_deps, run — в notify)
    local notify
    local run
    local rerun_scheduled = false

    local function clear_deps()
        for sig in pairs(deps) do
            if sig._subs then sig._subs[notify] = nil end
        end
        deps = {}
    end

    notify = function()
        if not active then return end
        if rerun_scheduled then return end
        rerun_scheduled = true
        -- defer'им чтобы серия :set() в одном тике дала ОДИН recompute
        task.defer(function()
            rerun_scheduled = false
            if not active then return end
            if current_inner_den then
                current_inner_den:close()
                current_inner_den = nil
            end
            run()
        end)
    end

    run = function()
        if not active then return end
        clear_deps()
        local prev = _current_reactive
        _current_reactive = function(sig)
            deps[sig] = true
            if not sig._subs then sig._subs = setmetatable({}, {__mode = "k"}) end
            sig._subs[notify] = true
        end
        -- ставим reactive-нору как контекст. Внутри fn она будет родителем для всего
        -- что fn создаст. При rerun — current_inner_den целиком убивается.
        local inner = d:_child("reactive")
        current_inner_den = inner
        local s = _stack()
        s[#s + 1] = inner
        local ok, err = pcall(fn)
        s[#s] = nil
        _current_reactive = prev
        if not ok then
            warn(("[Den reactive] error: %s"):format(err))
        end
    end

    d:_on_close_cb(function()
        active = false
        clear_deps()
        if current_inner_den then
            current_inner_den:close()
            current_inner_den = nil
        end
    end)

    run()
end

-- ── derived: signal вычисляемый из других signal'ов через reactive ──────────
-- local isC00 = derived(function() return killer:read() and killer:read().Name == "c00lkidd" end)
local function derived(fn)
    local out = signal(nil)
    -- derived живёт в текущей норе так же как reactive
    reactive(function()
        out:set(fn())
    end)
    return out
end

-- ── публичный экспорт ──────────────────────────────────────────────────────

return {
    Den          = Den,           -- builder factory + .root
    on           = on,
    once         = once,
    after        = after,
    defer        = defer,
    every        = every,
    on_step      = on_step,
    on_heartbeat = on_heartbeat,
    on_pre_sim   = on_pre_sim,
    own          = own,
    bind_to      = bind_to,
    until_       = until_,
    close_self   = close_self,
    inside       = inside,
    signal       = signal,
    reactive     = reactive,
    derived      = derived,
}
