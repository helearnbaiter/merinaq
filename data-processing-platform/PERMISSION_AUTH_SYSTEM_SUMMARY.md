# 权限与认证系统重构总结

## 项目概述
权限与认证系统基于 Casbin 框架实现，支持策略规则持久化存储到 PostgreSQL 数据库，并提供完整的策略维护功能以及业务数据与权限规则的关联管理。

## 系统架构

### 1. Casbin 服务层 (`src/services/casbin_service.rs`)
- **策略存储**: 使用 PostgreSQL 作为策略存储，通过 `DatabaseAdapter` 实现
- **模型定义**: 使用增强的 RBAC 模型，支持域（domain）概念
- **策略类型**: 支持 `p` (policy) 和 `g` (grouping) 类型策略
- **功能接口**:
  - 基础策略管理: `add_policy`, `remove_policy`, `get_policies`
  - 用户角色管理: `add_role_for_user`, `get_roles_for_user`
  - 业务关联管理: `add_data_source_permission`, `add_query_permission`
  - 批量操作: `add_policies`, `remove_policies`
  - 资源查询: `get_permissions_for_user`, `get_permissions_for_resource`

### 2. 处理器层 (`src/handlers/policy.rs`)
- **API 端点**:
  - `GET /policies` - 获取所有策略
  - `GET /policies/type/{policy_type}` - 按类型获取策略
  - `GET /policies/user/{user_id}` - 获取用户权限
  - `GET /policies/resource/{resource}` - 获取资源权限
  - `POST /policies` - 创建策略
  - `POST /policies/bulk` - 批量添加策略
  - `DELETE /policies/{id}` - 删除策略
  - `POST /policies/check` - 检查权限

### 3. 路由层 (`src/routes/v1/policies.rs`)
- 定义完整的策略管理路由
- 集成到 V1 API 版本

## 主要改进

### 1. 增强的 RBAC 模型
```ini
[request_definition]
r = sub, dom, obj, act

[policy_definition]
p = sub, dom, obj, act

[role_definition]
g = _, _, _
g2 = _, _

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = g(r.sub, p.sub, r.dom) && r.dom == p.dom && r.obj == p.obj && r.act == p.act
```

### 2. 域支持
- 引入域（domain）概念，支持多租户场景
- 支持 `enforce_with_domain` 方法

### 3. 扩展的策略管理功能
- 支持策略类型过滤 (`get_policy_by_type`)
- 支持用户和资源维度的权限查询
- 批量策略操作
- 业务数据关联管理（数据源、查询、BI仪表板）

### 4. 更好的错误处理
- 统一的错误码管理
- 详细的错误信息返回

## 业务数据关联管理

### 数据源权限管理
- `add_data_source_permission`: 为用户添加数据源访问权限
- `remove_data_source_permission`: 移除用户数据源访问权限
- `check_data_source_permission`: 检查用户数据源访问权限

### 查询权限管理
- `add_query_permission`: 为用户添加查询访问权限

### BI 仪表板权限管理
- `add_bi_permission`: 为用户添加 BI 仪表板访问权限

## 使用示例

### 添加数据源权限
```rust
casbin_service.add_data_source_permission("user123", 1, &["read", "write"]).await?;
```

### 检查权限
```rust
let allowed = casbin_service.enforce("user123", "data_source_1", "read").await?;
```

### 批量添加策略
```rust
let policies = vec![/* policy requests */];
casbin_service.add_policies(&policies).await?;
```

## 安全特性

### 中间件保护
- 认证中间件确保请求来源合法性
- 授权中间件基于策略进行访问控制
- 支持 RBAC 和 ABAC 模式

### 策略验证
- 策略参数验证
- 类型安全的策略操作
- 权限继承和传递性检查

## 扩展性设计

### 模块化架构
- 服务层与处理器层分离
- 便于单元测试和集成测试
- 支持策略模型的动态扩展

### 多租户支持
- 域（domain）概念支持多租户架构
- 策略隔离和权限边界控制

## 总结

重构后的权限与认证系统具有以下特点：

1. **完整性**: 实现了完整的策略 CRUD 操作
2. **扩展性**: 支持多租户、批量操作、业务关联
3. **安全性**: 通过中间件提供全面的安全保护
4. **易用性**: 提供丰富的 API 端点和清晰的错误处理
5. **持久化**: 策略规则持久化存储到 PostgreSQL

该系统能够满足企业级数据处理平台的权限管理需求，支持复杂的业务场景和多租户架构。