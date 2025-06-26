use streaming_iterator::StreamingIterator;
use tree_sitter::{Parser, Query, QueryCursor};

lazy_static::lazy_static! {
    static ref SYS_CALL_QUERY_BASH: Query = Query::new(
        &tree_sitter_bash::LANGUAGE.into(),
        r#"
        (command
            name: (command_name) @cmd_name
        )
        "#
    ).expect("Error creating query");
}

pub fn parse_bash_command(cmd: &str) -> Option<String> {
    // set up tree-sitter-bash parser
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_bash::LANGUAGE.into())
        .expect("Error loading Bash grammar");
    let tree = parser.parse(cmd, None).unwrap();
    let root = tree.root_node();
    let src = cmd.as_bytes();

    // run query to find command names
    let mut query_cursor = QueryCursor::new();
    let mut matches = query_cursor.matches(&SYS_CALL_QUERY_BASH, root, src);
    while let Some(m) = matches.next() {
        for capture in m.captures {
            if SYS_CALL_QUERY_BASH.capture_names()[capture.index as usize] == "cmd_name" {
                if let Ok(text) = capture.node.utf8_text(src) {
                    return Some(text.to_string());
                }
            }
        }
    }
    // If no command found, return None
    cmd.split_whitespace().next().map(|s| s.to_string())
}
