//! Casbin authorization service implementation
//! 
//! Handles policy management and permission checking using Casbin

use anyhow::Result;
use casbin::{CoreApi, CachedEnforcer, MgmtApi, Model, FileAdapter, DefaultModel};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::models::{Policy, CreatePolicyRequest};

pub struct CasbinService {
    enforcer: Arc<RwLock<CachedEnforcer>>,
}

impl CasbinService {
    pub async fn new(database_url: &str) -> Result<Self> {
        // Create a basic RBAC model
        let model = DefaultModel::from_str(
            r#"
            [request_definition]
            r = sub, obj, act

            [policy_definition]
            p = sub, obj, act

            [role_definition]
            g = _, _

            [policy_effect]
            e = some(where (p.eft == allow))

            [matchers]
            m = g(r.sub, p.sub) && r.obj == p.obj && r.act == p.act
            "#,
        ).await?;

        // For database adapter, we'd use the PostgreSQL adapter
        // In this example, we'll use file adapter for simplicity
        // In a production environment, use DatabaseAdapter with PostgreSQL
        let file_adapter = FileAdapter::new("config/policy.csv");
        
        let enforcer = CachedEnforcer::new(model, file_adapter).await?;
        
        // Add some default policies
        enforcer.add_policy(vec!["admin".to_string(), "*".to_string(), "*".to_string()]).await?;
        enforcer.add_policy(vec!["user".to_string(), "own".to_string(), "read".to_string()]).await?;
        
        Ok(CasbinService {
            enforcer: Arc::new(RwLock::new(enforcer)),
        })
    }

    pub async fn enforce(&self, subject: &str, resource: &str, action: &str) -> Result<bool> {
        let enforcer = self.enforcer.read().await;
        let result = enforcer.enforce(vec![subject, resource, action]).await?;
        Ok(result)
    }

    pub async fn add_policy(&self, policy: &CreatePolicyRequest) -> Result<bool> {
        let enforcer = self.enforcer.write().await;
        
        let mut policy_vec = vec![policy.v0.clone(), policy.v1.clone()];
        if let Some(v2) = &policy.v2 {
            policy_vec.push(v2.clone());
        }
        if let Some(v3) = &policy.v3 {
            policy_vec.push(v3.clone());
        }
        if let Some(v4) = &policy.v4 {
            policy_vec.push(v4.clone());
        }
        if let Some(v5) = &policy.v5 {
            policy_vec.push(v5.clone());
        }
        
        let result = enforcer.add_policy(policy_vec).await?;
        Ok(result)
    }

    pub async fn remove_policy(&self, policy: &CreatePolicyRequest) -> Result<bool> {
        let enforcer = self.enforcer.write().await;
        
        let mut policy_vec = vec![policy.v0.clone(), policy.v1.clone()];
        if let Some(v2) = &policy.v2 {
            policy_vec.push(v2.clone());
        }
        if let Some(v3) = &policy.v3 {
            policy_vec.push(v3.clone());
        }
        if let Some(v4) = &policy.v4 {
            policy_vec.push(v4.clone());
        }
        if let Some(v5) = &policy.v5 {
            policy_vec.push(v5.clone());
        }
        
        let result = enforcer.remove_policy(policy_vec).await?;
        Ok(result)
    }

    pub async fn get_policies(&self) -> Result<Vec<Policy>> {
        let enforcer = self.enforcer.read().await;
        let policy_rules = enforcer.get_policy();
        
        let mut policies = Vec::new();
        for rule in policy_rules {
            if rule.len() >= 2 {
                policies.push(Policy {
                    id: format!("policy_{}", uuid::Uuid::new_v4()),
                    ptype: "p".to_string(),
                    v0: rule[0].clone(),
                    v1: rule[1].clone(),
                    v2: rule.get(2).cloned(),
                    v3: rule.get(3).cloned(),
                    v4: rule.get(4).cloned(),
                    v5: rule.get(5).cloned(),
                });
            }
        }
        
        Ok(policies)
    }

    pub async fn add_role_for_user(&self, user: &str, role: &str) -> Result<bool> {
        let enforcer = self.enforcer.write().await;
        let result = enforcer.add_role_for_user(user, role).await?;
        Ok(result)
    }

    pub async fn delete_role_for_user(&self, user: &str, role: &str) -> Result<bool> {
        let enforcer = self.enforcer.write().await;
        let result = enforcer.delete_role_for_user(user, role).await?;
        Ok(result)
    }

    pub async fn get_roles_for_user(&self, user: &str) -> Result<Vec<String>> {
        let enforcer = self.enforcer.read().await;
        let roles = enforcer.get_roles_for_user(user).await?;
        Ok(roles)
    }
}