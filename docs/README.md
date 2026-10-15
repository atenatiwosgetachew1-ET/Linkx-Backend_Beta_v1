# LinkX Documentation Index

All operational, architectural, and handoff documentation for the LinkX distributed graph analytics platform.

---

## 🏗️ Architecture & Deployment

| Document | Purpose |
| :--- | :--- |
| [four_server_cluster_deployment_and_replication_handoff.md](four_server_cluster_deployment_and_replication_handoff.md) | **Master handoff** — end-to-end 4-server cluster deployment guide, secrets, verification |
| [server_communication_and_update_protocol.md](server_communication_and_update_protocol.md) | Network topology, port matrix, golden rules for command delivery |
| [multi_domain_development_strategy.md](multi_domain_development_strategy.md) | Phased roadmap for multi-domain (BDR/CDR) architecture and governance |
| [domain_onboarding_playbook.md](domain_onboarding_playbook.md) | Step-by-step guide for adding new analytical domains |
| [nbe_two_tier_hybrid_architecture_plan.md](nbe_two_tier_hybrid_architecture_plan.md) | NBE two-tier hybrid architecture design |
| [workspace_plan.md](workspace_plan.md) | Initial workspace and project structure plan |
| [service_split_handoff_and_load_audit.md](service_split_handoff_and_load_audit.md) | Service split from monolith to 4-server distributed architecture |
| [stress_testing_env_plan.md](stress_testing_env_plan.md) | Stress testing environment and methodology |

## 🔒 Security & Hardening

| Document | Purpose |
| :--- | :--- |
| [security_hardening_handoff.md](security_hardening_handoff.md) | Comprehensive security hardening procedures and audit trail |
| [rbac_auth_workflow.md](rbac_auth_workflow.md) | RBAC authentication workflow and permission model |
| [admin_auto_login_and_restore_handoff.md](admin_auto_login_and_restore_handoff.md) | Admin auto-login configuration and restore procedures |

## 📡 API & Integration

| Document | Purpose |
| :--- | :--- |
| [master_api_documentation.md](master_api_documentation.md) | Complete API endpoint documentation |
| [openapi.json](openapi.json) | OpenAPI 3.0 specification |
| [integration_contract.md](integration_contract.md) | External service integration contracts |
| [payload_validation.md](payload_validation.md) | Request payload validation rules |
| [api_gateway_config.md](api_gateway_config.md) | Nginx API gateway configuration |
| [api_configurable_rule_thresholds.md](api_configurable_rule_thresholds.md) | Configurable rule threshold API |
| [api_score_lineage.md](api_score_lineage.md) | Score lineage API documentation |

## 📊 Analysis Engine & Rules

| Document | Purpose |
| :--- | :--- |
| [analysis_engine_alignment_plan.md](analysis_engine_alignment_plan.md) | Analysis engine architecture alignment |
| [amount_threshold_calibration_and_sensitivity_guide.md](amount_threshold_calibration_and_sensitivity_guide.md) | Threshold calibration for AML detection sensitivity |
| [global_entity_classification_handoff.md](global_entity_classification_handoff.md) | Global entity classification system |
| [edge_semantic_visualization_handoff.md](edge_semantic_visualization_handoff.md) | Edge semantic visualization for graph UI |
| [enterprise_accounts_sample.md](enterprise_accounts_sample.md) | Enterprise account sample data reference |

## ⚠️ Risk Scoring & xVigilance

| Document | Purpose |
| :--- | :--- |
| [risk_scoring_link_analysis_handoff.md](risk_scoring_link_analysis_handoff.md) | Risk scoring via link analysis integration |
| [risk_scoring_dynamic_config_handoff.md](risk_scoring_dynamic_config_handoff.md) | Dynamic risk scoring configuration |
| [Risk_Scoring_Team_API_Spec.md](Risk_Scoring_Team_API_Spec.md) | Risk scoring team API specification |
| [xvigilance_architecture_plan.md](xvigilance_architecture_plan.md) | xVigilance autonomous detective engine architecture |
| [xvigilance_system_evaluation.md](xvigilance_system_evaluation.md) | xVigilance system evaluation and assessment |
| [xvigilance_frontend_dashboard_handoff.md](xvigilance_frontend_dashboard_handoff.md) | xVigilance frontend dashboard handoff |
| [xvigilance_risk_scoring_integration_handoff.md](xvigilance_risk_scoring_integration_handoff.md) | xVigilance ↔ risk scoring pipeline integration |
| [api_xvigilance_clock_rewind.md](api_xvigilance_clock_rewind.md) | xVigilance clock rewind API |
| [api_xvigilance_pause_troubleshoot.md](api_xvigilance_pause_troubleshoot.md) | xVigilance pause/resume troubleshooting |

## 📈 Reporting & Observability

| Document | Purpose |
| :--- | :--- |
| [report_integration_plan.md](report_integration_plan.md) | Report generation integration plan |
| [report_pipeline_implementation_plan.md](report_pipeline_implementation_plan.md) | Report pipeline implementation details |
| [iso_backend_metrics_assessment.md](iso_backend_metrics_assessment.md) | ISO metrics compliance assessment |
| [otel_agent_installation_handoff.md](otel_agent_installation_handoff.md) | OpenTelemetry agent installation |
| [linkx_aggregator_integration_guide.md](linkx_aggregator_integration_guide.md) | Aggregator integration guide |

## 📋 Operational

| Document | Purpose |
| :--- | :--- |
| [to_restore.md](to_restore.md) | Items pending restoration |
| [server1_stress_visuals.md](server1_stress_visuals.md) | Server 1 stress test visualization results |

---

*Last updated: October 2026*
