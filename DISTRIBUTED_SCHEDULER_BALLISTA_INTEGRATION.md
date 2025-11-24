# DataFusion Ballista 集成方案

## 当前状态分析

### 1. 现有分布式调度器实现
当前项目中的 `distributed_scheduler.rs` 文件已经实现了基础的分布式查询调度功能：

- ✅ **查询分解**: 基础框架已实现，但实际分解逻辑未完成（仅创建一个子查询）
- ✅ **并行执行**: 实现了并行执行逻辑
- ✅ **结果聚合**: 实现了结果聚合框架
- ✅ **故障恢复**: 实现了基础的熔断器机制，但缺少节点故障重调度

### 2. 当前实现的不足
- 查询分解逻辑未完成，所有查询都被当作单一子查询处理
- 缺少真正的跨节点并行执行，使用的是本地查询引擎
- 故障恢复机制不完整，缺少任务重调度功能
- 没有集成真正的分布式执行引擎

## DataFusion Ballista 集成方案

### 1. 什么是 DataFusion Ballista
DataFusion Ballista 是一个基于 Apache Arrow 的分布式计算平台，提供了：
- 分布式查询执行引擎
- 任务调度和资源管理
- 容错和故障恢复机制
- 任务分片和并行执行

### 2. 集成步骤

#### 步骤 1: 更新 Cargo.toml
```toml
[dependencies]
# 添加 Ballista 依赖
ballista = "0.18"
ballista-core = "0.18"
ballista-executor = "0.18"
ballista-scheduler = "0.18"
```

#### 步骤 2: 重构分布式调度器
```rust
use ballista::context::BallistaContext;
use ballista::prelude::{BallistaConfig, BALLISTA_DEFAULT_SCHEDULER_NAME};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub struct BallistaDistributedQueryScheduler {
    context: Arc<BallistaContext>,
    // 保留现有功能
    cluster_nodes: Arc<RwLock<HashMap<String, ClusterNode>>>,
    // ... 其他字段
}

impl BallistaDistributedQueryScheduler {
    pub async fn new(scheduler_url: &str) -> Result<Self> {
        let config = BallistaConfig::builder()
            .set("ballista.shuffle.staging_dir", "/tmp")
            .build()?;
        
        let context = BallistaContext::remote("localhost", 50050, &config).await?;
        
        Ok(Self {
            context: Arc::new(context),
            // ... 初始化其他字段
        })
    }

    async fn analyze_query(&self, query: &str, query_id: &str) -> Result<DistributedQueryPlan> {
        // 使用 Ballista 的查询分析能力进行真正的查询分解
        let logical_plan = self.context.logical_plan(query).await?;
        let optimized_plan = self.context.optimize(&logical_plan)?;
        
        // 将优化后的计划分解为可并行执行的子任务
        let subqueries = self.decompose_query_plan(&optimized_plan, query_id)?;
        
        Ok(DistributedQueryPlan {
            query_id: query_id.to_string(),
            original_query: query.to_string(),
            subqueries,
            execution_strategy: self.determine_execution_strategy(&optimized_plan)?,
        })
    }
}
```

#### 步骤 3: 实现查询分解功能
```rust
impl BallistaDistributedQueryScheduler {
    fn decompose_query_plan(&self, plan: &LogicalPlan, query_id: &str) -> Result<Vec<SubQuery>> {
        // 分析查询计划并将其分解为可分布执行的子任务
        let mut subqueries = Vec::new();
        
        // 识别可并行执行的操作（如表扫描、过滤等）
        match plan {
            LogicalPlan::TableScan(scan) => {
                // 如果是表扫描，可以根据分区进行分解
                if let Some(table_partition_cols) = self.get_table_partitions(&scan.table_name) {
                    for (i, partition) in table_partition_cols.iter().enumerate() {
                        let subquery_sql = format!(
                            "{} WHERE {} = '{}'", 
                            plan.display_indent(), 
                            partition.0, 
                            partition.1
                        );
                        subqueries.push(SubQuery {
                            id: format!("{}_{}", query_id, i),
                            sql: subquery_sql,
                            target_node: None,
                            dependencies: Vec::new(),
                        });
                    }
                }
            }
            LogicalPlan::Join(join) => {
                // 对于连接操作，可以分解为多个子连接
                // 根据连接类型和数据分布策略进行分解
                // ...
            }
            _ => {
                // 对于其他操作，根据数据分布进行分解
                // ...
            }
        }
        
        Ok(subqueries)
    }
}
```

#### 步骤 4: 实现故障恢复机制
```rust
impl BallistaDistributedQueryScheduler {
    async fn execute_with_fault_tolerance(&self, plan: &DistributedQueryPlan) -> Result<QueryExecutionResult> {
        let mut results = HashMap::new();
        let mut failed_subqueries = Vec::new();
        
        // 尝试执行所有子查询
        for subquery in &plan.subqueries {
            match self.execute_subquery_with_retry(subquery).await {
                Ok(result) => {
                    results.insert(subquery.id.clone(), result);
                }
                Err(e) => {
                    tracing::warn!("Subquery {} failed: {}, will retry", subquery.id, e);
                    failed_subqueries.push(subquery.clone());
                }
            }
        }
        
        // 对失败的子查询进行重调度
        for subquery in failed_subqueries {
            let retry_result = self.reschedule_failed_subquery(&subquery).await?;
            results.insert(subquery.id.clone(), retry_result);
        }
        
        self.aggregate_results(&results).await
    }
    
    async fn reschedule_failed_subquery(&self, subquery: &SubQuery) -> Result<Vec<RecordBatch>> {
        // 找到一个健康的节点重新执行子查询
        let healthy_node = self.find_healthy_node().await?;
        
        // 更新子查询的目标节点
        let mut new_subquery = subquery.clone();
        new_subquery.target_node = Some(healthy_node.id);
        
        // 执行子查询
        self.execute_subquery(&new_subquery).await
    }
}
```

### 3. 重构建议

#### 重构 1: 模块化设计
将当前的分布式调度器重构为以下模块：
- `query_analyzer`: 负责查询分解和优化
- `task_scheduler`: 负责任务调度和节点选择
- `result_aggregator`: 负责结果聚合
- `fault_recovery`: 负责故障检测和恢复

#### 重构 2: 配置管理
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedSchedulerConfig {
    pub scheduler_url: String,
    pub executor_nodes: Vec<String>,
    pub query_timeout_secs: u64,
    pub max_concurrent_queries: usize,
    pub enable_query_cache: bool,
    pub cache_ttl_secs: u64,
    pub fault_recovery_enabled: bool,
    pub max_retry_attempts: u32,
}
```

#### 重构 3: 状态管理
```rust
pub struct DistributedQueryState {
    pub query_id: String,
    pub status: QueryStatus,
    pub progress: QueryProgress,
    pub start_time: std::time::Instant,
    pub end_time: Option<std::time::Instant>,
    pub error: Option<String>,
    pub metrics: QueryMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryProgress {
    pub total_subqueries: usize,
    pub completed_subqueries: usize,
    pub failed_subqueries: usize,
    pub active_subqueries: usize,
}
```

### 4. 实现计划

#### 阶段 1: 基础集成 (Week 1)
- 更新 Cargo.toml 添加 Ballista 依赖
- 创建 Ballista 集成的基础结构
- 实现简单的 Ballista 上下文连接

#### 阶段 2: 查询分解 (Week 2)
- 实现基于 Ballista 的查询计划分析
- 完善查询分解逻辑
- 添加查询优化策略

#### 阶段 3: 并行执行 (Week 3)
- 实现跨节点并行执行
- 优化任务调度算法
- 添加负载均衡策略

#### 阶段 4: 故障恢复 (Week 4)
- 实现完整的故障检测机制
- 添加任务重调度功能
- 完善熔断器和健康检查

#### 阶段 5: 测试和优化 (Week 5)
- 编写集成测试
- 性能基准测试
- 优化查询执行效率

### 5. 部署架构

```
Client Applications
        |
        v
   REST/GraphQL API
        |
        v
   Query Engine (Ballista Context)
        |
        v
   +-------------------+
   |  Ballista Scheduler  |
   |  - Task scheduling   |
   |  - Resource mgmt     |
   |  - Fault recovery    |
   +-------------------+
        |        |
        |        v
        |   +------------------+
        |   |  Ballista Executors  |
        |   |  - Distributed exec  |
        |   |  - Data processing   |
        |   +------------------+
        v
   +------------------+
   |  Data Sources     |
   |  - PostgreSQL     |
   |  - MySQL          |
   |  - Parquet files  |
   |  - Iceberg tables |
   +------------------+
```

### 6. 性能优化考虑

- 查询结果缓存策略
- 数据本地性优化
- 自适应查询执行
- 内存管理和溢出处理
- 网络传输优化

### 7. 监控和运维

- 查询执行指标收集
- 节点健康监控
- 资源使用监控
- 性能分析工具
- 日志和调试支持

通过以上集成方案，可以充分利用 DataFusion Ballista 的分布式计算能力，同时保留当前项目中的高级功能和定制化逻辑。