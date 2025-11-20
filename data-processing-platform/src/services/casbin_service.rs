//! Casbin authorization service implementation
//! 
//! Handles policy management and permission checking using Casbin

use anyhow::Result;
use casbin::{CoreApi, CachedEnforcer, MgmtApi, Model, DefaultModel};
use casbin::DatabaseAdapter;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::models::{Policy, CreatePolicyRequest};

pub struct CasbinService {
    enforcer: Arc<RwLock<CachedEnforcer>>,
}

impl CasbinService {
    pub async fn new(database_url: &str) -> Result<Self> {
        // Create an enhanced RBAC model with domain support
        let model = DefaultModel::from_str(
            r#"
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
            "#,
        ).await?;

        // Use PostgreSQL adapter for storing policies in database
        let adapter = DatabaseAdapter::new(database_url).await?;
        
        let enforcer = CachedEnforcer::new(model, adapter).await?;
        
        // Load existing policies from database
        enforcer.load_policy().await?;
        
        Ok(CasbinService {
            enforcer: Arc::new(RwLock::new(enforcer)),
        })
    }

    pub async fn enforce(&self, subject: &str, resource: &str, action: &str) -> Result<bool> {
        let enforcer = self.enforcer.read().await;
        let result = enforcer.enforce(vec![subject, "default", resource, action]).await?;
        Ok(result)
    }

    pub async fn enforce_with_domain(&self, subject: &str, domain: &str, resource: &str, action: &str) -> Result<bool> {
        let enforcer = self.enforcer.read().await;
        let result = enforcer.enforce(vec![subject, domain, resource, action]).await?;
        Ok(result)
    }

    pub async fn add_policy(&self, policy: &CreatePolicyRequest) -> Result<bool> {
        let enforcer = self.enforcer.write().await;
        
        let mut policy_vec = if policy.ptype == "g" {
            // For role inheritance rules, we need at least 2 parameters
            vec![policy.v0.clone(), policy.v1.clone()]
        } else {
            // For regular policies, use domain-based format
            vec![policy.v0.clone(), 
                 policy.v1.clone(), // domain
                 policy.v2.clone().unwrap_or_else(|| "default".to_string()), 
                 policy.v3.clone().unwrap_or_else(|| "read".to_string())]
        };
        
        // Add additional parameters if provided (for g-type policies)
        if policy.ptype == "g" {
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
        }
        
        let result = enforcer.add_policy(policy_vec).await?;
        Ok(result)
    }

    pub async fn remove_policy(&self, policy: &CreatePolicyRequest) -> Result<bool> {
        let enforcer = self.enforcer.write().await;
        
        let mut policy_vec = if policy.ptype == "g" {
            vec![policy.v0.clone(), policy.v1.clone()]
        } else {
            vec![policy.v0.clone(), 
                 policy.v1.clone(), 
                 policy.v2.clone().unwrap_or_else(|| "default".to_string()), 
                 policy.v3.clone().unwrap_or_else(|| "read".to_string())]
        };
        
        // Add additional parameters if provided (for g-type policies)
        if policy.ptype == "g" {
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
        }
        
        let result = enforcer.remove_policy(policy_vec).await?;
        Ok(result)
    }

    pub async fn remove_policy_exact(&self, ptype: &str, v0: &str, v1: &str, v2: Option<&str>, v3: Option<&str>, v4: Option<&str>, v5: Option<&str>) -> Result<bool> {
        let enforcer = self.enforcer.write().await;
        
        let mut policy_vec = if ptype == "g" {
            vec![v0.to_string(), v1.to_string()]
        } else {
            vec![v0.to_string(), 
                 v1.to_string(), 
                 v2.unwrap_or("default").to_string(), 
                 v3.unwrap_or("read").to_string()]
        };
        
        // Add additional parameters if provided (for g-type policies)
        if ptype == "g" {
            if let Some(val) = v2 {
                policy_vec.push(val.to_string());
            }
            if let Some(val) = v3 {
                policy_vec.push(val.to_string());
            }
            if let Some(val) = v4 {
                policy_vec.push(val.to_string());
            }
            if let Some(val) = v5 {
                policy_vec.push(val.to_string());
            }
        }
        
        let result = enforcer.remove_filtered_policy(0, policy_vec).await?;
        Ok(result)
    }

    pub async fn get_policies(&self) -> Result<Vec<Policy>> {
        let enforcer = self.enforcer.read().await;
        let policy_rules = enforcer.get_policy();
        
        let mut policies = Vec::new();
        for rule in policy_rules {
            if rule.len() >= 2 {
                // Generate a consistent ID based on the policy content for identification
                let id_content = format!("{}-{}-{}", rule[0], rule.get(1).unwrap_or(&"".to_string()), rule.get(2).unwrap_or(&"".to_string()));
                let id = format!("policy_{}", &id_content[..std::cmp::min(id_content.len(), 20)]);
                
                policies.push(Policy {
                    id,
                    ptype: "p".to_string(),
                    v0: rule[0].clone(),
                    v1: rule.get(1).unwrap_or(&"".to_string()).clone(),
                    v2: rule.get(2).cloned(),
                    v3: rule.get(3).cloned(),
                    v4: rule.get(4).cloned(),
                    v5: rule.get(5).cloned(),
                });
            }
        }
        
        // Also add role inheritance rules
        let role_rules = enforcer.get_grouping_policy();
        for rule in role_rules {
            if rule.len() >= 2 {
                let id_content = format!("g-{}-{}", rule[0], rule.get(1).unwrap_or(&"".to_string()));
                let id = format!("policy_{}", &id_content[..std::cmp::min(id_content.len(), 20)]);
                
                policies.push(Policy {
                    id,
                    ptype: "g".to_string(),
                    v0: rule[0].clone(),
                    v1: rule.get(1).unwrap_or(&"".to_string()).clone(),
                    v2: rule.get(2).cloned(),
                    v3: rule.get(3).cloned(),
                    v4: rule.get(4).cloned(),
                    v5: rule.get(5).cloned(),
                });
            }
        }
        
        Ok(policies)
    }

    pub async fn get_policy_by_type(&self, ptype: &str) -> Result<Vec<Policy>> {
        let enforcer = self.enforcer.read().await;
        let policy_rules = match ptype {
            "p" => enforcer.get_policy(),
            "g" => enforcer.get_grouping_policy(),
            _ => return Err(anyhow::anyhow!("Invalid policy type: {}", ptype)),
        };
        
        let mut policies = Vec::new();
        for rule in policy_rules {
            if rule.len() >= 2 {
                let id_content = format!("{}-{}-{}", ptype, rule[0], rule.get(1).unwrap_or(&"".to_string()));
                let id = format!("policy_{}-{}", ptype, &id_content[..std::cmp::min(id_content.len(), 20)]);
                
                policies.push(Policy {
                    id,
                    ptype: ptype.to_string(),
                    v0: rule[0].clone(),
                    v1: rule.get(1).unwrap_or(&"".to_string()).clone(),
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

    pub async fn add_role_for_user_in_domain(&self, user: &str, role: &str, domain: &str) -> Result<bool> {
        let enforcer = self.enforcer.write().await;
        let result = enforcer.add_grouping_policy(vec![user, role, domain]).await?;
        Ok(result)
    }

    pub async fn delete_role_for_user(&self, user: &str, role: &str) -> Result<bool> {
        let enforcer = self.enforcer.write().await;
        let result = enforcer.delete_role_for_user(user, role).await?;
        Ok(result)
    }

    pub async fn delete_role_for_user_in_domain(&self, user: &str, role: &str, domain: &str) -> Result<bool> {
        let enforcer = self.enforcer.write().await;
        let result = enforcer.remove_grouping_policy(vec![user, role, domain]).await?;
        Ok(result)
    }

    pub async fn get_roles_for_user(&self, user: &str) -> Result<Vec<String>> {
        let enforcer = self.enforcer.read().await;
        let roles = enforcer.get_roles_for_user(user).await?;
        Ok(roles)
    }

    pub async fn get_roles_for_user_in_domain(&self, user: &str, domain: &str) -> Result<Vec<String>> {
        let enforcer = self.enforcer.read().await;
        let roles = enforcer.get_roles_for_user_in_domain(user, domain).await?;
        Ok(roles)
    }

    pub async fn get_users_for_role(&self, role: &str) -> Result<Vec<String>> {
        let enforcer = self.enforcer.read().await;
        let users = enforcer.get_users_for_role(role).await?;
        Ok(users)
    }

    pub async fn get_users_for_role_in_domain(&self, role: &str, domain: &str) -> Result<Vec<String>> {
        let enforcer = self.enforcer.read().await;
        let users = enforcer.get_users_for_role_in_domain(role, domain).await?;
        Ok(users)
    }

    // Business data association methods
    
    /// Add permissions for a user to access a specific data source
    pub async fn add_data_source_permission(&self, user: &str, data_source_id: i32, actions: &[&str]) -> Result<bool> {
        let mut success = true;
        for action in actions {
            let policy = CreatePolicyRequest {
                ptype: "p".to_string(),
                v0: user.to_string(),
                v1: "default".to_string(), // domain
                v2: Some(format!("data_source_{}", data_source_id)),
                v3: Some(action.to_string()),
                v4: None,
                v5: None,
            };
            if !self.add_policy(&policy).await? {
                success = false;
            }
        }
        Ok(success)
    }

    /// Remove permissions for a user to access a specific data source
    pub async fn remove_data_source_permission(&self, user: &str, data_source_id: i32, actions: &[&str]) -> Result<bool> {
        let mut success = true;
        for action in actions {
            let policy = CreatePolicyRequest {
                ptype: "p".to_string(),
                v0: user.to_string(),
                v1: "default".to_string(), // domain
                v2: Some(format!("data_source_{}", data_source_id)),
                v3: Some(action.to_string()),
                v4: None,
                v5: None,
            };
            if !self.remove_policy(&policy).await? {
                success = false;
            }
        }
        Ok(success)
    }

    /// Check if a user has permission to perform an action on a specific data source
    pub async fn check_data_source_permission(&self, user: &str, data_source_id: i32, action: &str) -> Result<bool> {
        self.enforce(user, &format!("data_source_{}", data_source_id), action).await
    }

    /// Add permissions for a user to access specific queries
    pub async fn add_query_permission(&self, user: &str, query_id: i32, action: &str) -> Result<bool> {
        let policy = CreatePolicyRequest {
            ptype: "p".to_string(),
            v0: user.to_string(),
            v1: "default".to_string(), // domain
            v2: Some(format!("query_{}", query_id)),
            v3: Some(action.to_string()),
            v4: None,
            v5: None,
        };
        self.add_policy(&policy).await
    }

    /// Add permissions for a user to access specific BI dashboards
    pub async fn add_bi_permission(&self, user: &str, dashboard_id: i32, action: &str) -> Result<bool> {
        let policy = CreatePolicyRequest {
            ptype: "p".to_string(),
            v0: user.to_string(),
            v1: "default".to_string(), // domain
            v2: Some(format!("bi_{}", dashboard_id)),
            v3: Some(action.to_string()),
            v4: None,
            v5: None,
        };
        self.add_policy(&policy).await
    }

    /// Get all permissions for a specific user
    pub async fn get_permissions_for_user(&self, user: &str) -> Result<Vec<Policy>> {
        let enforcer = self.enforcer.read().await;
        let policies = enforcer.get_implicit_permissions_for_user(user).await?;
        
        let mut result = Vec::new();
        for policy in policies {
            if policy.len() >= 2 {
                let id_content = format!("user-{}-{}", user, policy[0]);
                let id = format!("perm_{}-{}", user, &id_content[..std::cmp::min(id_content.len(), 20)]);
                
                result.push(Policy {
                    id,
                    ptype: "p".to_string(),
                    v0: policy[0].clone(), // subject
                    v1: policy.get(1).unwrap_or(&"".to_string()).clone(), // domain
                    v2: policy.get(2).cloned(), // object
                    v3: policy.get(3).cloned(), // action
                    v4: policy.get(4).cloned(),
                    v5: policy.get(5).cloned(),
                });
            }
        }
        
        Ok(result)
    }

    /// Get all permissions for a specific resource
    pub async fn get_permissions_for_resource(&self, resource: &str) -> Result<Vec<Policy>> {
        let enforcer = self.enforcer.read().await;
        let policies = enforcer.get_filtered_policy(2, vec![resource.to_string()]); // Filter by resource (obj)
        
        let mut result = Vec::new();
        for policy in policies {
            if policy.len() >= 2 {
                let id_content = format!("resource-{}-{}", resource, policy[0]);
                let id = format!("perm_res_{}-{}", resource, &id_content[..std::cmp::min(id_content.len(), 20)]);
                
                result.push(Policy {
                    id,
                    ptype: "p".to_string(),
                    v0: policy[0].clone(), // subject
                    v1: policy.get(1).unwrap_or(&"".to_string()).clone(), // domain
                    v2: policy.get(2).cloned(), // object
                    v3: policy.get(3).cloned(), // action
                    v4: policy.get(4).cloned(),
                    v5: policy.get(5).cloned(),
                });
            }
        }
        
        Ok(result)
    }

    /// Bulk add policies
    pub async fn add_policies(&self, policies: &[CreatePolicyRequest]) -> Result<bool> {
        let enforcer = self.enforcer.write().await;
        let mut policy_rules = Vec::new();
        
        for policy in policies {
            let mut policy_vec = if policy.ptype == "g" {
                vec![policy.v0.clone(), policy.v1.clone()]
            } else {
                vec![policy.v0.clone(), 
                     policy.v1.clone(), 
                     policy.v2.clone().unwrap_or_else(|| "default".to_string()), 
                     policy.v3.clone().unwrap_or_else(|| "read".to_string())]
            };
            
            // Add additional parameters if provided (for g-type policies)
            if policy.ptype == "g" {
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
            }
            
            policy_rules.push(policy_vec);
        }
        
        let result = enforcer.add_policies(policy_rules).await?;
        Ok(result)
    }

    /// Bulk remove policies
    pub async fn remove_policies(&self, policies: &[CreatePolicyRequest]) -> Result<bool> {
        let enforcer = self.enforcer.write().await;
        let mut policy_rules = Vec::new();
        
        for policy in policies {
            let mut policy_vec = if policy.ptype == "g" {
                vec![policy.v0.clone(), policy.v1.clone()]
            } else {
                vec![policy.v0.clone(), 
                     policy.v1.clone(), 
                     policy.v2.clone().unwrap_or_else(|| "default".to_string()), 
                     policy.v3.clone().unwrap_or_else(|| "read".to_string())]
            };
            
            // Add additional parameters if provided (for g-type policies)
            if policy.ptype == "g" {
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
            }
            
            policy_rules.push(policy_vec);
        }
        
        let result = enforcer.remove_policies(policy_rules).await?;
        Ok(result)
    }
}