use crate::jinja::DbtRef;
use crate::project::ProjectManifest;
use tower_lsp::lsp_types::{Diagnostic, DiagnosticSeverity, Position, Range};
use ropey::Rope;

pub fn validate_refs(
    refs: &[(DbtRef, std::ops::Range<usize>)],
    manifest: Option<&ProjectManifest>,
    rope: &Rope,
    tree: Option<&tree_sitter::Tree>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    
    // 1. Syntax Errors from Tree-sitter
    if let Some(tree) = tree {
        collect_syntax_errors(tree.root_node(), &mut diagnostics);
    }

    // 2. Ref Validation (Semantic)
    if let Some(manifest) = manifest {
        for (dbt_ref, range) in refs {
            let is_valid = match dbt_ref {
                DbtRef::Model(name) => manifest.models.contains_key(name),
                DbtRef::Source(_src, _tbl) => true, // TODO: Validate sources
            };

            if !is_valid {
                let start_line = rope.byte_to_line(range.start);
                let start_char = range.start - rope.line_to_byte(start_line);
                let end_line = rope.byte_to_line(range.end);
                let end_char = range.end - rope.line_to_byte(end_line);

                let msg = match dbt_ref {
                    DbtRef::Model(name) => format!("Model '{}' not found in project.", name),
                    DbtRef::Source(s, t) => format!("Source '{}.{}' not found.", s, t),
                };

                diagnostics.push(Diagnostic {
                    range: Range {
                        start: Position::new(start_line as u32, start_char as u32),
                        end: Position::new(end_line as u32, end_char as u32),
                    },
                    severity: Some(DiagnosticSeverity::ERROR),
                    code: None,
                    code_description: None,
                    source: Some("dbt-lsp".to_string()),
                    message: msg,
                    related_information: None,
                    tags: None,
                    data: None,
                });
            }
        }
    }

    diagnostics
}

fn collect_syntax_errors(node: tree_sitter::Node, diagnostics: &mut Vec<Diagnostic>) {
    if node.has_error() {
        if node.is_error() || node.is_missing() {
            let start = node.start_position();
            let end = node.end_position();
            diagnostics.push(Diagnostic {
                range: Range {
                    start: Position::new(start.row as u32, start.column as u32),
                    end: Position::new(end.row as u32, end.column as u32),
                },
                severity: Some(DiagnosticSeverity::ERROR),
                message: if node.is_missing() {
                    format!("Missing: {}", node.kind())
                } else {
                    "SQL Syntax Error".to_string()
                },
                source: Some("dbt-lsp".to_string()),
                ..Diagnostic::default()
            });
        } else {
            for i in 0..node.child_count() {
                collect_syntax_errors(node.child(i).unwrap(), diagnostics);
            }
        }
    }
}
