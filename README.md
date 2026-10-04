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
- 有限容量与预留：实例元数据键 `capacity`（整数，缺省为 1，非法或负数视为 0）表示可同时承载的容量单位；每个单位同一时刻只能归一个会话所有。只读 `Recheck` 仍然只做预览，但会在 `Honorable` 中说明候选里此刻还能兑现多少实例；只有预留或已确认的容量才算保障，多个会话不会把同一份容量各自当成可用。
- 预留生命周期：`Reserve(ttl)` 针对候选选择暂时持有容量并给出到期时间；有效期内 `Confirm()` 转为已确认使用，`CancelReservation()` 提前释放，不确认则到期自动失效。重复确认回放同一次决定、不重复占用；重复取消不多释放。`ReleaseConfirmed()` 显式归还已确认容量。
- 保障失效：租约到期、主动退出、旧化身重登或能力减少时，历史选择记录与占用事实保留不变；`Recheck` 的 `Guaranteed` 只列出当前仍受保障的已确认实例，`LostGuarantee` 逐个说明失去保障的范围与原因。任何会话的最低数量都不会挪用别的会话已确认的容量。
- 替换与不中断：容量感知会话的 `Recheck` 返回 `Switch` 计划，说明确认切换时旧占用何时释放、新预留在何时生效，以及切换后是否仍满足最低数量（`Uninterrupted`）；会降到最低数量以下时 `Confirm()` 返回 `ErrDegradationNeedsAccept`，必须由 `ConfirmAcceptingDegradation()` 明确接受降级，否则保留原占用不切换。
- 恢复一致性：`Save`/`OpenRegistry` 持久化存活的容量归属；重开后已到期的预留不会复活，已确认占用继续有效。客户端可用 `ResumeSelectionSession(id, criteria)` 以原会话 ID 恢复，恢复后收到重复的确认/取消通知仍然幂等，同一份容量不会出现互相矛盾的归属。未调用过 `Reserve` 的只读选择会话行为完全不变。
- 可控时钟：`FakeClock` 支持冻结时间和手动 `Advance`，定时器只随时钟推进触发，用于确定性地验证续租、重连、扫描与重启一致性。

## 测试

```sh
go test -race ./...
```

测试覆盖：并发续租、租约失效、主动退出区分、旧身份重启隔离、订阅断线重连、历史落后触发快照，以及假时钟驱动的后台扫描。
