use regex::{Captures, Regex};
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DbtRef {
    Model(String),
    Source(String, String), // source_name, table_name
    Macro(String),
}

fn re_ref() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r#"(?xs)\{\{\s*[-]?\s*ref\s*\(\s*['"]([a-zA-Z0-9_\.]+)['"]\s*\)\s*[-]?\s*\}\}"#).unwrap())
}

fn re_source() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r#"(?xs)\{\{\s*[-]?\s*source\s*\(\s*['"]([a-zA-Z0-9_\.]+)['"]\s*,\s*['"]([a-zA-Z0-9_\.]+)['"]\s*\)\s*[-]?\s*\}\}"#).unwrap())
}

pub fn is_macro_file(text: &str) -> bool {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r#"(?s)\{[%-]\s*macro\s+"#).unwrap());
    re.is_match(text)
}

fn re_macro_call() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r#"(?xs)\{[{%]\s*[-]?\s*(?:do\s+)?([a-zA-Z0-9_\.]+)\s*\("#).unwrap())
}

fn re_generic_jinja() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?s)\{\{.*?\}\}").unwrap())
}

fn re_jinja_block() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?s)\{%.*?%\}").unwrap())
}

fn re_jinja_comment() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?s)\{#.*?#\}").unwrap())
}

fn re_dataform() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?s)\$\{.*?\}").unwrap())
}

fn preserve_newlines_replace(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if c == '\n' {
            out.push('\n');
        } else {
            out.push(' ');
        }
    }
    out
}

/// Preprocesses SQL text by replacing Jinja constructs with valid SQL identifiers
/// so that Tree-sitter can parse the structure.
/// Cruatilly, this preserves the byte length of the text so that tree-sitter ranges
/// map 1:1 to the original text.
pub fn preprocess_for_parsing(text: &str) -> String {
    let result = text.to_string();

    // 1. Replace Refs: {{ ref('model') }} -> __DBT_REF_model_______
    let result = re_ref().replace_all(&result, |caps: &Captures| {
        let full_match = &caps[0];
        let model_name = &caps[1];
        let desired_ident = format!("__DBT_REF_{}", model_name);
        
        if desired_ident.len() > full_match.len() {
             preserve_newlines_replace(full_match)
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
             preserve_newlines_replace(full_match)
        } else {
             let padding = " ".repeat(full_match.len() - desired_ident.len());
             format!("{}{}", desired_ident, padding)
        }
    });


    // Handle {{ this }} specifically, preserving length but making it a valid identifier or empty
    static RE_THIS: OnceLock<Regex> = OnceLock::new();
    let re_this = RE_THIS.get_or_init(|| Regex::new(r"(?i)\{\{\s*this\s*\}\}").unwrap());
    let result = re_this.replace_all(&result, |caps: &Captures| {
        // preserve length: {{ this }} -> ________
        preserve_newlines_replace(&caps[0])
    });

    let result = re_jinja_block().replace_all(&result, |caps: &Captures| {
       preserve_newlines_replace(&caps[0])
    });

    let result = re_jinja_comment().replace_all(&result, |caps: &Captures| {
        preserve_newlines_replace(&caps[0])
    });
    
    // Handle # comments (which might confuse sqlparser if not strictly BigQuery mode or if placed weirdly)
    // We replace # with -- to keep it as a comment but safer for standard SQL parsers if needed
    // OR just replace with spaces if we want to ignore them.
    // Let's replace with spaces to be safe and preserve layout.
    static RE_HASH_COMMENT: OnceLock<Regex> = OnceLock::new();
    let re_hash_comment = RE_HASH_COMMENT.get_or_init(|| Regex::new(r"(?m)^\s*#.*$|(?m)\s+#.*$").unwrap());
    let result = re_hash_comment.replace_all(&result, |caps: &Captures| {
         preserve_newlines_replace(&caps[0])
    });

    let result = re_generic_jinja().replace_all(&result, |caps: &Captures| {
         preserve_newlines_replace(&caps[0])
    });

    let result = re_dataform().replace_all(&result, |caps: &Captures| {
        preserve_newlines_replace(&caps[0])
    });

    result.to_string()
}

pub fn extract_refs(text: &str) -> Vec<(DbtRef, std::ops::Range<usize>)> {
    let mut refs = Vec::new();
    
    for cap in re_ref().captures_iter(text) {
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

    for cap in re_macro_call().captures_iter(text) {
        if let Some(full) = cap.get(0) {
            if let Some(m) = cap.get(1) {
                let name = m.as_str();
                if name != "ref" && name != "source" && name != "config" && name != "var" && name != "env_var" {
                    refs.push((DbtRef::Macro(name.to_string()), full.range()));
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
    fn test_preprocess_preserves_length_and_newlines() {
        let input = "select * from {{ \nref('my_table') \n}} where id = {{ config(...) }}";
        let output = preprocess_for_parsing(input);
        
        assert_eq!(input.len(), output.len(), "Length must be preserved");
        assert_eq!(input.lines().count(), output.lines().count(), "Line count must be preserved");
        
        println!("Input:  {:?}\nOutput: {:?}", input, output);
    }
}
