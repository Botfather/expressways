use std::collections::{HashMap, HashSet};

use expressways_protocol::Action;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DefaultDecision {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub principal: String,
    pub resource: String,
    pub actions: Vec<Action>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
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

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PolicyConfigError {
    #[error("policy default decision must be `deny`")]
    DefaultAllow,
    #[error("policy has {actual} rules, exceeding the limit of {limit}")]
    TooManyRules { actual: usize, limit: usize },
    #[error("policy rule {index} {field} pattern must contain 1..={limit} bytes")]
    InvalidPatternLength {
        index: usize,
        field: &'static str,
        limit: usize,
    },
    #[error("policy rule {index} {field} pattern may only use `*` as the final character")]
    InvalidWildcard { index: usize, field: &'static str },
    #[error("policy rule {index} must contain at least one action")]
    EmptyActions { index: usize },
    #[error("policy rule {index} contains duplicate action `{action}`")]
    DuplicateAction { index: usize, action: String },
}

const MAX_POLICY_RULES: usize = 10_000;
const MAX_PRINCIPAL_PATTERN_BYTES: usize = 256;
const MAX_RESOURCE_PATTERN_BYTES: usize = 2_048;

#[derive(Debug, Clone)]
struct CompiledRule {
    principal: String,
    resource: String,
    label: String,
}

#[derive(Debug, Clone)]
pub struct PolicyEngine {
    rules_by_action: HashMap<Action, Vec<CompiledRule>>,
}

impl PolicyEngine {
    pub fn new(config: PolicyConfig) -> Result<Self, PolicyConfigError> {
        if config.default_decision != DefaultDecision::Deny {
            return Err(PolicyConfigError::DefaultAllow);
        }
        if config.rules.len() > MAX_POLICY_RULES {
            return Err(PolicyConfigError::TooManyRules {
                actual: config.rules.len(),
                limit: MAX_POLICY_RULES,
            });
        }

        let mut rules_by_action: HashMap<Action, Vec<CompiledRule>> = HashMap::new();
        for (index, rule) in config.rules.into_iter().enumerate() {
            validate_pattern(
                &rule.principal,
                index,
                "principal",
                MAX_PRINCIPAL_PATTERN_BYTES,
            )?;
            validate_pattern(
                &rule.resource,
                index,
                "resource",
                MAX_RESOURCE_PATTERN_BYTES,
            )?;
            if rule.actions.is_empty() {
                return Err(PolicyConfigError::EmptyActions { index });
            }

            let mut actions = HashSet::with_capacity(rule.actions.len());
            for action in &rule.actions {
                if !actions.insert(action.clone()) {
                    return Err(PolicyConfigError::DuplicateAction {
                        index,
                        action: action.as_str().to_owned(),
                    });
                }
            }

            let compiled = CompiledRule {
                label: format!("{} -> {}", rule.principal, rule.resource),
                principal: rule.principal,
                resource: rule.resource,
            };
            for action in rule.actions {
                rules_by_action
                    .entry(action)
                    .or_default()
                    .push(compiled.clone());
            }
        }

        Ok(Self { rules_by_action })
    }

    pub fn evaluate(&self, principal: &str, resource: &str, action: &Action) -> Evaluation {
        if let Some(rules) = self.rules_by_action.get(action) {
            for rule in rules {
                if !rule_matches(rule, principal, resource) {
                    continue;
                }
                return Evaluation {
                    decision: Decision::Allow,
                    matched_rule: Some(rule.label.clone()),
                };
            }
        }

        Evaluation {
            decision: Decision::Deny,
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

fn rule_matches(rule: &CompiledRule, principal: &str, resource: &str) -> bool {
    pattern_matches(&rule.principal, principal) && pattern_matches(&rule.resource, resource)
}

fn validate_pattern(
    pattern: &str,
    index: usize,
    field: &'static str,
    limit: usize,
) -> Result<(), PolicyConfigError> {
    if pattern.is_empty() || pattern.len() > limit {
        return Err(PolicyConfigError::InvalidPatternLength {
            index,
            field,
            limit,
        });
    }
    if pattern.strip_suffix('*').unwrap_or(pattern).contains('*') {
        return Err(PolicyConfigError::InvalidWildcard { index, field });
    }
    Ok(())
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
        let engine = PolicyEngine::new(config()).expect("valid policy");

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
        let engine = PolicyEngine::new(config()).expect("valid policy");

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

    #[test]
    fn default_allow_is_rejected() {
        let mut candidate = config();
        candidate.default_decision = DefaultDecision::Allow;
        assert_eq!(
            PolicyEngine::new(candidate).expect_err("default allow must fail"),
            PolicyConfigError::DefaultAllow
        );
    }

    #[test]
    fn malformed_patterns_and_duplicate_actions_are_rejected() {
        let mut candidate = config();
        candidate.rules[0].resource = "system:*:admin".to_owned();
        assert!(matches!(
            PolicyEngine::new(candidate),
            Err(PolicyConfigError::InvalidWildcard { .. })
        ));

        let mut candidate = config();
        candidate.rules[0].actions = vec![Action::Admin, Action::Admin];
        assert!(matches!(
            PolicyEngine::new(candidate),
            Err(PolicyConfigError::DuplicateAction { .. })
        ));
    }
}
