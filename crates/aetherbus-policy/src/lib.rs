use aetherbus_protocol::Action;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DefaultDecision {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Rule {
    pub principal: String,
    pub resource: String,
    pub actions: Vec<Action>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolicyConfig {
    pub default_decision: DefaultDecision,
    #[serde(default)]
    pub rules: Vec<Rule>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Evaluation {
    pub decision: Decision,
    pub matched_rule: Option<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PolicyError {
    #[error("principal `{principal}` is not authorized for action `{action}` on `{resource}`")]
    Unauthorized {
        principal: String,
        action: String,
        resource: String,
    },
}

#[derive(Debug, Clone)]
pub struct PolicyEngine {
    config: PolicyConfig,
}

impl PolicyEngine {
    pub fn new(config: PolicyConfig) -> Self {
        Self { config }
    }

    pub fn evaluate(&self, principal: &str, resource: &str, action: &Action) -> Evaluation {
        for rule in &self.config.rules {
            if rule_matches(rule, principal, resource, action) {
                return Evaluation {
                    decision: Decision::Allow,
                    matched_rule: Some(format!("{} -> {}", rule.principal, rule.resource)),
                };
            }
        }

        let decision = match self.config.default_decision {
            DefaultDecision::Allow => Decision::Allow,
            DefaultDecision::Deny => Decision::Deny,
        };

        Evaluation {
            decision,
            matched_rule: None,
        }
    }

    pub fn authorize(
        &self,
        principal: &str,
        resource: &str,
        action: &Action,
    ) -> Result<Evaluation, PolicyError> {
        let evaluation = self.evaluate(principal, resource, action);
        match evaluation.decision {
            Decision::Allow => Ok(evaluation),
            Decision::Deny => Err(PolicyError::Unauthorized {
                principal: principal.to_owned(),
                action: action.as_str().to_owned(),
                resource: resource.to_owned(),
            }),
        }
    }
}

fn rule_matches(rule: &Rule, principal: &str, resource: &str, action: &Action) -> bool {
    pattern_matches(&rule.principal, principal)
        && pattern_matches(&rule.resource, resource)
        && rule.actions.iter().any(|candidate| candidate == action)
}

fn pattern_matches(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    if let Some(prefix) = pattern.strip_suffix('*') {
        return value.starts_with(prefix);
    }

    pattern == value
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> PolicyConfig {
        PolicyConfig {
            default_decision: DefaultDecision::Deny,
            rules: vec![
                Rule {
                    principal: "local:developer".to_owned(),
                    resource: "system:*".to_owned(),
                    actions: vec![Action::Admin, Action::Health],
                },
                Rule {
                    principal: "local:agent-*".to_owned(),
                    resource: "topic:tasks".to_owned(),
                    actions: vec![Action::Publish, Action::Consume],
                },
            ],
        }
    }

    #[test]
    fn exact_and_prefix_rules_are_supported() {
        let engine = PolicyEngine::new(config());

        let evaluation = engine
            .authorize("local:agent-alpha", "topic:tasks", &Action::Publish)
            .expect("authorized");

        assert_eq!(evaluation.decision, Decision::Allow);
        assert_eq!(
            evaluation.matched_rule,
            Some("local:agent-* -> topic:tasks".to_owned())
        );
    }

    #[test]
    fn missing_rule_is_denied() {
        let engine = PolicyEngine::new(config());

        let error = engine
            .authorize("local:agent-alpha", "topic:secret", &Action::Publish)
            .expect_err("denied");

        assert_eq!(
            error,
            PolicyError::Unauthorized {
                principal: "local:agent-alpha".to_owned(),
                action: "publish".to_owned(),
                resource: "topic:secret".to_owned(),
            }
        );
    }
}
