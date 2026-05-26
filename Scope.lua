-- Scope.lua — lightweight lifecycle manager для exploit runtime
-- Версия: финальная (все фиксы из обсуждения включены)
--
-- ФИКСЫ:
--   1. _children утечка    — дочерний scope удаляет себя из parent O(1) swap-delete
--   2. Once race condition — conn присваивается ДО Connect через wrapper-таблицу
--   3. Interval баг        — cancel через флаг alive, task.cancel на thread убран
--   4. BindToRenderStep    — не добавляется в Add (возвращает nil), только через Cleanup
--   5. voicelineScope      — пересоздаётся при unblock, не накапливает мёртвые записи
--   6. _watcherConns утечка — snd.Destroying-коннект шёл в общий массив и не удалялся
--                             при уничтожении звука. Фикс: beatScope:Once(snd.Destroying)
--                             само-дисконнектится при срабатывании → нет мёртвых записей.
--
--      БЫЛО (v51 attachSound строка 3143):
--        table.insert(_watcherConns, snd.Destroying:Connect(function()
--            _watchedSounds[snd] = nil
--        end))
--
--      СТАЛО:
--        beatScope:Once(snd.Destroying, function()
--            _watchedSounds[snd] = nil
--        end)

local Scope = {}
Scope.__index = Scope
Scope.__call  = function(self, obj) return self:Add(obj) end

-- ── internal helpers ──────────────────────────────────────────────────────────

local function safeCall(fn, scope, ...)
    local ok, err = pcall(fn, ...)
    if not ok then
        local name = (scope and scope._debugName) or "?"
        warn(("[Scope:%s] cleanup error: %s"):format(name, tostring(err)))
    end
end

local function cleanObj(obj, scope)
    local t = typeof(obj)
    if     t == "RBXScriptConnection" then pcall(function() obj:Disconnect() end)
    elseif t == "Instance"            then pcall(function() obj:Destroy()    end)
    elseif type(obj) == "function"    then safeCall(obj, scope)
    elseif type(obj) == "thread"      then pcall(task.cancel, obj)
    end
end

-- ── constructor ───────────────────────────────────────────────────────────────

function Scope.new(parent, debugName)
    local self = setmetatable({
        _tasks      = {},
        _children   = {},
        _dead       = false,
        _destroying = false,
        _debugName  = debugName,
        _parent     = parent,
        _parentIdx  = nil,
    }, Scope)

    if parent then
        table.insert(parent._children, self)
        self._parentIdx = #parent._children
    end

    return self
end

-- ── state ─────────────────────────────────────────────────────────────────────

function Scope:IsDead()
    return self._dead or self._destroying
end

-- ── tracking ──────────────────────────────────────────────────────────────────

-- Добавляет объект в scope. При Destroy вызовет Disconnect/Destroy/fn/cancel.
-- Если scope уже мёртв — чистит немедленно, не копит.
function Scope:Add(obj)
    if self:IsDead() then
        cleanObj(obj, self)
        return obj
    end
    table.insert(self._tasks, obj)
    return obj
end

-- Алиас для читаемости когда добавляем cleanup-функцию.
function Scope:Cleanup(fn)
    return self:Add(fn)
end

-- Подключает signal:Connect(fn) и сохраняет connection в scope.
function Scope:Connect(signal, fn)
    return self:Add(signal:Connect(fn))
end

-- ФИX 2: оригинальный Once присваивал conn ПОСЛЕ Connect. Если сигнал стрелял
-- синхронно — conn был nil внутри callback'а и Disconnect не происходил.
-- Решение: wrapper-таблица как мутабельный контейнер виден замыканию сразу.
function Scope:Once(signal, fn)
    local handle = { conn = nil }

    handle.conn = signal:Connect(function(...)
        local c = handle.conn
        if c then
            c:Disconnect()
            handle.conn = nil
        end
        if self:IsDead() then return end
        fn(...)
    end)

    return self:Add(handle.conn)
end

-- ── async helpers ─────────────────────────────────────────────────────────────

-- task.delay с проверкой живости scope. Thread добавляется в scope.
function Scope:Delay(t, fn)
    local thread = task.delay(t, function()
        if self:IsDead() then return end
        fn()
    end)
    return self:Add(thread)
end

-- task.defer с проверкой живости scope.
function Scope:Defer(fn)
    local thread = task.defer(function()
        if self:IsDead() then return end
        fn()
    end)
    return self:Add(thread)
end

-- ФИX 3: task.cancel на coroutine работает только если тот заблокирован в yield.
-- Если fn() выполнялась в момент cancel — отмена не происходила, loop продолжался.
-- Решение: управляем через флаг alive. В _tasks только cleanup-функция, не thread.
function Scope:Interval(interval, fn)
    local alive = true

    task.spawn(function()
        while alive and not self:IsDead() do
            task.wait(interval)
            if alive and not self:IsDead() then
                fn()
            end
        end
    end)

    self:Cleanup(function()
        alive = false
    end)

    -- возвращаем stop-функцию для ручного управления
    return function() alive = false end
end

-- ── instance binding ──────────────────────────────────────────────────────────

-- Уничтожает scope когда instance удаляется из игры.
function Scope:BindToInstance(instance)
    if not instance or self:IsDead() then return end
    return self:Connect(instance.Destroying, function()
        self:Destroy()
    end)
end

-- BindToRenderStep с автоматическим UnbindFromRenderStep при Destroy.
-- RunService:BindToRenderStep возвращает nil — поэтому трекается через Cleanup,
-- не через Add. Безопасно вызывать несколько раз с разными именами.
function Scope:BindToRenderStep(name, priority, fn)
    if self:IsDead() then return end
    local RunService = game:GetService("RunService")
    RunService:BindToRenderStep(name, priority, fn)
    self:Cleanup(function()
        pcall(function() RunService:UnbindFromRenderStep(name) end)
    end)
end

-- ── child scopes ──────────────────────────────────────────────────────────────

-- Создаёт дочерний scope. Уничтожается вместе с родителем (LIFO).
function Scope:Child(debugName)
    return Scope.new(self, debugName)
end

-- ── destroy ───────────────────────────────────────────────────────────────────

function Scope:Destroy()
    if self._dead or self._destroying then return end
    self._destroying = true

    -- ФИX 1: убираем себя из parent._children swap-удалением O(1).
    -- Без этого parent накапливает мёртвые ссылки на уничтоженные дочерние scope'ы.
    local p = self._parent
    if p and not p._dead then
        local idx  = self._parentIdx
        local last = #p._children
        if idx and idx <= last then
            if idx ~= last then
                local moved    = p._children[last]
                p._children[idx] = moved
                moved._parentIdx = idx
            end
            p._children[last] = nil
        end
    end
    self._parent    = nil
    self._parentIdx = nil

    -- snapshot перед обнулением — дети и задачи могут вызвать Destroy рекурсивно
    local children = self._children
    local tasks    = self._tasks
    self._children = {}
    self._tasks    = {}

    -- сначала рекурсивно уничтожаем детей (LIFO)
    for i = #children, 1, -1 do
        local child = children[i]
        if child and not child._dead then
            child._parent    = nil
            child._parentIdx = nil
            child:Destroy()
        end
    end

    -- затем чистим свои задачи (LIFO)
    for i = #tasks, 1, -1 do
        cleanObj(tasks[i], self)
    end

    self._dead       = true
    self._destroying = false
end

return Scope
