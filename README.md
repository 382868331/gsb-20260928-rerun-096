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
- 可控时钟：`FakeClock` 支持冻结时间和手动 `Advance`，定时器只随时钟推进触发，用于确定性地验证续租、重连、扫描与重启一致性。

## 测试

```sh
go test -race ./...
```

测试覆盖：并发续租、租约失效、主动退出区分、旧身份重启隔离、订阅断线重连、历史落后触发快照，以及假时钟驱动的后台扫描。
