# 服务发现运行核心（Go）

第一轮的进程内服务发现核心，不依赖外部注册中心。后续调用框架可直接从这里读取虚拟服务的可用实例地址。

```go
reg := discovery.NewRegistry()

// 实例登记，获得带 token 的租约。
h, _ := reg.Register(discovery.Instance{
    Service: "checkout",
    ID:      "instance-uuid",
    Addr:    "10.0.0.8:9000",
}, 10*time.Second)

// 续租：必须同时匹配 service、instance id 与本次登记返回的 token。
deadline, err := reg.Heartbeat(h.Instance.Service, h.Instance.ID, h.Token)

// 主动退出；租约超时则由 Sweep/RunSweeper 记录为 expired。
reg.Deregister(h.Instance.Service, h.Instance.ID, h.Token)
```

## 关键语义

- 实例身份：每次登记返回随机租约 token。进程重启即新身份/新 token；旧进程的心跳、就绪切换、排空或退出都返回 `ErrInvalidToken`，不能续上新租约，旧 token 也不能挤掉新实例。
- 租约：`Heartbeat` 在截止时间前移；超过截止时间后 `Sweep()` 或后台 `RunSweeper` 将其标记为 `expired`。租约有效性不依赖清理先运行：`SetReady`/`Drain`/`Heartbeat`/`Deregister` 在租约已过期时当场补记 `expired` 并拒绝操作，`ReadyInstances`/`Snapshot`/`ChangesSince`/`Subscribe`/`Save` 查询时刻也会先内联过期，保证任何时刻读到的都是同一有效实例集合。主动退出标记为 `deregistered`，两者在变更事件和 `Removal` 审计记录中分开保存。
- 版本：所有登记/退出产生目录内连续、单调递增的版本号。
- 就绪状态：登记不等于可接收请求。实例依次经历 `preparing`、`ready`、`draining`：`SetReady` 产生就绪版本事件后才出现在 `ReadyInstances`，`Drain` 停止接流但保留租约；`ready`/`draining` 都是带版本的目录变更，排空后不能再回到就绪。
- 分区：`Instance.Partition` 给实例分区，`Subscribe(ctx, version, partitions...)` 与 `ChangesSince(version, partitions...)` 可只订阅部分分区。所有分区共用同一全局单调版本；慢订阅者错过保留窗口时拿到的是其分区在**单一一致版本**上的完整快照，不会把不同分区的旧增量拼在一起。
- 增量订阅：`ChangesSince(version)` 返回该版本之后的变化。版本 0 或已被压缩时返回完整 `Snapshot`；落后太多的订阅者必须以快照重建本地目录，不会拿到被截断的事件流。
- 推送订阅：`Subscribe(ctx, version)` 先给基线（或落后时的快照），再推送后续变化；断线重连时传入最后完整处理的版本即可。
- 持久化：`Save(dir)` 原子落盘租约、就绪状态、保留事件与离职审计；`OpenRegistry(dir, ...)` 重开时恢复仍然有效的租约与状态，版本继续单调增长。打开时已到截止时间的实例不会复活，而是补记 `expired` 事件与审计，因此恢复旧文件不能让过期实例重新上线。
- 能力变更：`UpdateMetadata(service, id, token, meta)` 在不换身份的前提下原地更新元数据（如 `capabilities`、`region`），产生带版本的 `updated` 变更事件；同样需要当前租约 token，租约已过期则当场补记 `expired` 并拒绝。
- 可解释选择会话：`NewSelectionSession(Criteria{Service, Capabilities, PreferredRegion, MinAvailable})` 开启一次客户端选择会话。每条 `Selection` 记录保存当时的目录版本与完整快照（`Basis`）、每个实例被采用或排除的原因（`Verdicts`）、按偏好区域排序的选中集（`Picked`），以及条件不满足时的 `Shortfall` 说明。能力与区域来自实例元数据键 `capabilities`（逗号分隔）与 `region`；选择只读取本地目录，不向真实服务发请求。
- 会话演进：`Recheck()`/`RecheckWith(新条件)` 针对当前目录给出候选选择，并把已确认记录中失效的实例列入 `Stale`（区分 `lease expired`、`deregistered`、能力缺失、未就绪），失效实例不会出现在候选可用集中；`Confirm()` 才切换到候选并追加进不可变历史，暂缓切换则保留原记录与短缺原因。目录重开后失效原因一致，因为离职审计随 `Save` 持久化。
- 选择比较：`CompareSelections(a, b)` 说明两次选择的差异来自实例状态变化（`Lost`/`Gained` 及每个实例的原因）还是用户条件变化（`CriteriaChanges`）；历史记录只增不改，新条件不会补写进旧记录。
- 容量预留：实例元数据键 `capacity` 声明可用容量（缺省为 1）。选择会话区分三种状态：仅供预览的候选（`Recheck` 的 `Candidate`，多个会话可能同时看到同一份容量）、暂时预留（`Reserve(ttl)` 产生的 `reserved` 占用，带截止时间）、已确认使用（`Confirm` 后的 `confirmed` 占用，无 expiry）。`Recheck` 的 `Honorable` 说明当前还能兑现多少实例，`Guaranteed`/`LostGuarantee` 说明已确认容量中仍然有保障与失去保障的范围及原因。容量占用（`Hold`）是所有权的唯一事实来源，同一份容量不会同时属于两个会话，也绝不会为补足某会话的最低数量而挪用另一会话已确认的容量。
- 预留生命周期：`Reserve` 后可在有效期内 `Confirm` 转为确认使用，或 `CancelReservation` 提前释放，或任其到期自动失效（与租约一样在所有读写路径内联处理）。确认结果的 `Picked`/`Satisfied` 只表示本会话实际拥有的容量，部分预留不会把完整候选报告为已满足；完整候选保存在 `CandidatePicked`，实际兑现数量保存在 `Honored`。重复 `Confirm` 返回同一次决定、重复 `CancelReservation` 是空操作，不会多占或多释放。预留已失效才 `Confirm` 时，仅当容量仍然空闲才重新获取；没有任何可兑现容量时返回 `ErrCapacityUnavailable`，只够部分兑现时必须显式调用 `ConfirmAcceptingDegradation`。
- 替换与降级：`Recheck`/`RecheckWith` 的 `Switch` 说明旧占用在 Confirm 提交时释放、新预留在 Confirm 时生效，以及 `Uninterrupted` 是否满足不中断条件；条件不足时 `Confirm` 返回 `ErrDegradationNeedsAccept`，需用户用 `ConfirmAcceptingDegradation` 明确接受降级，否则保持原选择与原占用。
- 降级记录与补足计划：被明确接受的降级决定携带不可变的 `Degradation` 记录（承诺数量、兑现数量、缺失实例与接受时间）。此后 `Recheck` 的 `TopUp` 计划把同一决定的候选、拒绝与已接受降级范围关联起来：`Promised` 是历史上曾承诺的完整集合，`GuaranteedNow` 是现在仍可兑现的部分，`LostGuarantee` 带原因（租约过期、主动退出、占用被释放），`Missing` 是仍然缺少的数量，`Fillable` 列出此刻有空闲容量可补的实例；过期容量单独审计，绝不并入当前保障。
- 容量补足：`TopUpReserve(ttl)` 在同一会话内补足缺失容量，只占当前空闲单元且数量不超过缺口，不替换已确认的有效占用，也不预先占满候选列表，其他会话继续公平争用剩余容量。补足预留是独立的预留组，有自己的到期时间，`CancelTopUp` 单独撤销，不影响切换预留与已确认占用。`ConfirmTopUp` 把补足预留转为确认使用并追加历史记录：`TopUp.Amends` 指向被补的决定，`Added`/`StillMissing`/`ExpiredSincePromise` 区分本次新增、仍缺失与承诺后已过期的部分；预留已过期时只重新获取仍然空闲的容量，绝不抢占其他会话。重开后重复的补足确认返回同一次决定，不重复分配、不改写历史。
- 容量持久化与恢复：`Save`/`OpenRegistry` 一并落盘与恢复存活占用和会话的确认、取消、选择历史、待决候选与最近决定；停机期间过期的预留不会被复活。`ResumeSelectionSession(sessionID, ...)` 让客户端在目录恢复后以原身份继续，恢复后收到的重复确认仍返回同一次决定，不会重新生成或追加历史，也不会产生矛盾归属。不要求容量预留的只读选择会话行为不变。
- 可控时钟：`FakeClock` 支持冻结时间和手动 `Advance`，定时器只随时钟推进触发，用于确定性地验证续租、重连、扫描与重启一致性。

## 测试

```sh
go test -race ./...
```

测试覆盖：并发续租、租约失效、主动退出区分、旧身份重启隔离、订阅断线重连、历史落后触发快照、假时钟驱动的后台扫描，以及容量预留的争用单一归属、预留到期与确认交错、降级切换门槛、部分接受后的补足（新实例加入、原实例到期、公平争用、重开后重复补足通知一致）和目录重开后的重复通知一致性。
