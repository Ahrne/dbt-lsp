use regex::{Captures, Regex};
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DbtRef {
    Model(String),
    Source(String, String), // source_name, table_name
}

fn re_ref() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r#"(?x)\{\{\s*ref\s*\(\s*['"]([a-zA-Z0-9_]+)['"]\s*\)\s*\}\}"#).unwrap())
}

fn re_source() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r#"(?x)\{\{\s*source\s*\(\s*['"]([a-zA-Z0-9_]+)['"]\s*,\s*['"]([a-zA-Z0-9_]+)['"]\s*\)\s*\}\}"#).unwrap())
}

fn re_generic_jinja() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\{\{.*?\}\}").unwrap())
}

fn re_jinja_block() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\{%.*?%\}").unwrap())
}

fn re_jinja_comment() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\{#.*?#\}").unwrap())
}

/// Preprocesses SQL text by replacing Jinja constructs with valid SQL identifiers
/// so that Tree-sitter can parse the structure.
/// Cruatilly, this preserves the byte length of the text so that tree-sitter ranges
/// map 1:1 to the original text.
pub fn preprocess_for_parsing(text: &str) -> String {
    let result = text.to_string();

    // Helper to replace range with new content padded to same length
    fn replace_preserve_len(text: &mut String, range: std::ops::Range<usize>, replacement_prefix: &str) {
        let len = range.len();
        let prefix_len = replacement_prefix.len();
        
        if len < prefix_len {
            // Edge case: Original text too short. Just use generic spacer.
            // But `{{ref('a')}}` is always > `__DBT_REF_a` usually.
            // If it happens, we can't emit a valid identifier easily without growing.
            // We just replace with whitespace to avoid syntax error, or partial.
            let spaces = " ".repeat(len);
             text.replace_range(range, &spaces);
        } else {
            let mut substitution = replacement_prefix.to_string();
            substitution.push_str(&" ".repeat(len - prefix_len));
            text.replace_range(range, &substitution);
        }
    }

    // Capture offsets first to avoid shifting indices during iteration if we weren't careful.
    // But since we preserve length, indices are stable!
    
    // 1. Replace Refs: {{ ref('model') }} -> __DBT_REF_model_______
    // We strictly use `replace_all` logic but we need to generate string based on match logic.
    // Regex `replace_all` with closure is easiest if we return same length string.
    
    let result = re_ref().replace_all(&result, |caps: &Captures| {
        let full_match = &caps[0];
        let model_name = &caps[1];
        let desired_ident = format!("__DBT_REF_{}", model_name);
        
        if desired_ident.len() > full_match.len() {
             " ".repeat(full_match.len())
        } else {
             let padding = " ".repeat(full_match.len() - desired_ident.len());
             format!("{}{}", desired_ident, padding)
        }
    });

    let result = re_source().replace_all(&result, |caps: &Captures| {
        let full_match = &caps[0];
        let src_name = &caps[1];
        let tbl_name = &caps[2];
        let desired_ident = format!("__DBT_SRC_{}_{}", src_name, tbl_name);
         
        if desired_ident.len() > full_match.len() {
             " ".repeat(full_match.len())
        } else {
             let padding = " ".repeat(full_match.len() - desired_ident.len());
             format!("{}{}", desired_ident, padding)
        }
    });

    // Replace Control Flow / generic blocks with SQL comments to preserve length
    // {% ... %} -> /* ... */
    let result = re_jinja_block().replace_all(&result, |caps: &Captures| {
       let len = caps[0].len();
       if len >= 4 {
           let content_len = len - 4; // /* */ is 4 chars
           format!("/*{}*/", " ".repeat(content_len))
       } else {
           " ".repeat(len)
       }
    });

    // Replace Comments with spaces
    // {# ... #} -> "        "
    let result = re_jinja_comment().replace_all(&result, |caps: &Captures| {
        " ".repeat(caps[0].len())
    });
    
    // Generic {{ ... }} that wasn't caught by ref/source.
    let result = re_generic_jinja().replace_all(&result, |caps: &Captures| {
         let full_match = &caps[0];
         let desired_ident = "__DBT_EXPR";
         if desired_ident.len() > full_match.len() {
              // fallback to comment
              if full_match.len() >= 4 {
                  format!("/*{}*/", " ".repeat(full_match.len()-4))
              } else {
                  " ".repeat(full_match.len())
              }
         } else {
              let padding = " ".repeat(full_match.len() - desired_ident.len());
              format!("{}{}", desired_ident, padding)
         }
    });

    result.to_string()
}

pub fn extract_refs(text: &str) -> Vec<(DbtRef, std::ops::Range<usize>)> {
    let mut refs = Vec::new();
    
    for cap in re_ref().captures_iter(text) {
        // Group 0 is the full match: {{ ref(...) }}
        if let Some(full) = cap.get(0) {
            if let Some(m) = cap.get(1) {
                refs.push((DbtRef::Model(m.as_str().to_string()), full.range()));
            }
        }
    }
    
    for cap in re_source().captures_iter(text) {
        if let Some(full) = cap.get(0) {
            if let Some(m1) = cap.get(1) {
                if let Some(m2) = cap.get(2) {
                    refs.push((DbtRef::Source(m1.as_str().to_string(), m2.as_str().to_string()), full.range()));
                }
            }
        }
    }
    
    refs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preprocess_preserves_length() {
        let input = "select * from {{ ref('my_table') }} where id = {{ var('x') }}";
        let output = preprocess_for_parsing(input);
        
        assert_eq!(input.len(), output.len(), "Length must be preserved");
        assert!(output.contains("__DBT_REF_my_table"));
        assert!(output.contains("__DBT_EXPR"));
        
        println!("Input:  {}\nOutput: {}", input, output);
    }
    
    #[test]
    fn test_extract_refs() {
        let input = "select * from {{ ref('my_table') }} join {{ source('raw', 'users') }}";
        let refs = extract_refs(input);
        
        assert_eq!(refs.len(), 2);
        assert!(refs.contains(&DbtRef::Model("my_table".to_string())));
        assert!(refs.contains(&DbtRef::Source("raw".to_string(), "users".to_string())));
    }
}
