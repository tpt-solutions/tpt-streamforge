//! Tests for the config loader (env substitution, located errors), the
//! validate/explain/dry-run/manifest/watch flow, and the data tools
//! (init, doctor, convert, diff, schema drift).

use std::path::Path;
use std::time::Duration;

use tpt_stream_cli::config::{check_spec, explain_spec, substitute_env, SubstError};
use tpt_stream_cli::manifest::{rfc3339_utc, sha256_hex};
use tpt_stream_cli::schema_json::{definitions, pipeline_schema, pipeline_schema_string};
use tpt_stream_cli::tools;
use tpt_stream_cli::{
    completions_command, explain_command, man_command, parse_pipeline_toml_with_env,
    run_command_default, run_with, validate_command, RunOptions,
};

fn write(path: &Path, content: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, content).unwrap();
}

fn toml_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn no_env(_: &str) -> Option<String> {
    None
}

fn parse_err(text: &str) -> String {
    format!(
        "{:#}",
        parse_pipeline_toml_with_env(text, &no_env).unwrap_err()
    )
}

const SAMPLE: &str = "id,region,amount\n1,north,5\n2,south,\n3,north,7\n";

// ---------------------------------------------------------------------------
// ${VAR} substitution
// ---------------------------------------------------------------------------

#[test]
fn substitution_expands_defaults_and_escapes() {
    let env = |k: &str| match k {
        "A" => Some("alpha".to_string()),
        "EMPTY" => Some(String::new()),
        _ => None,
    };
    assert_eq!(substitute_env("x-${A}-y", &env).unwrap(), "x-alpha-y");
    assert_eq!(substitute_env("${B:-fallback}", &env).unwrap(), "fallback");
    assert_eq!(substitute_env("${A:-fallback}", &env).unwrap(), "alpha");
    // `:-` also covers set-but-empty (shell semantics); plain `${EMPTY}` allows it.
    assert_eq!(substitute_env("${EMPTY:-d}", &env).unwrap(), "d");
    assert_eq!(substitute_env("[${EMPTY}]", &env).unwrap(), "[]");
    // Escape and lone dollars.
    assert_eq!(substitute_env("$${A}", &env).unwrap(), "${A}");
    assert_eq!(
        substitute_env("cost: $5 or $$", &env).unwrap(),
        "cost: $5 or $$"
    );
    assert_eq!(
        substitute_env("${A}$${A}${A}", &env).unwrap(),
        "alpha${A}alpha"
    );
}

#[test]
fn substitution_reports_unset_and_malformed() {
    assert_eq!(
        substitute_env("${NOPE}", &no_env).unwrap_err(),
        SubstError::Unset("NOPE".into())
    );
    assert!(matches!(
        substitute_env("${1BAD}", &no_env).unwrap_err(),
        SubstError::Malformed(_)
    ));
    assert!(matches!(
        substitute_env("${A", &no_env).unwrap_err(),
        SubstError::Malformed(_)
    ));
    assert!(matches!(
        substitute_env("${}", &no_env).unwrap_err(),
        SubstError::Malformed(_)
    ));
}

#[test]
fn env_vars_expand_inside_pipeline_values_but_not_keys() {
    let env = |k: &str| match k {
        "DB" => Some("s3cret".to_string()),
        "OUT" => Some("result.csv".to_string()),
        _ => None,
    };
    let text = r#"
[source.postgres]
connection = "host=db password=${DB}"
query = "select 1"

[[stages]]
filter = "price > 5"

[sink]
csv = "${OUT}"
"#;
    let spec = parse_pipeline_toml_with_env(text, &env).unwrap();
    match spec.source {
        tpt_stream_cli::SourceSpec::Postgres(p) => {
            assert_eq!(p.connection, "host=db password=s3cret");
        }
        other => panic!("{other:?}"),
    }
    match spec.sink.unwrap() {
        tpt_stream_cli::SinkSpec::Csv(s) => assert_eq!(s.path, "result.csv"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn unset_env_var_error_names_the_variable_and_line() {
    let text = "[source.csv]\npath = \"in.csv\"\n\n[sink.csv]\npath = \"${MISSING_OUT}/o.csv\"\n";
    let err = parse_err(text);
    assert!(err.contains("MISSING_OUT"), "{err}");
    assert!(err.contains("is not set"), "{err}");
    assert!(err.contains("line 5"), "{err}");
    assert!(err.contains("sink"), "{err}");
    // The hint shows both escape hatches.
    assert!(err.contains("${MISSING_OUT:-default}"), "{err}");
    assert!(err.contains("$${MISSING_OUT}"), "{err}");
}

#[test]
fn escaped_dollar_brace_stays_literal_in_a_spec() {
    let text = "[source.csv]\npath = \"in.csv\"\n\n[[stages]]\nfilter = \"name == '$${HOME}'\"\n";
    let spec = parse_pipeline_toml_with_env(text, &no_env).unwrap();
    match &spec.stages[0] {
        tpt_stream_cli::StageSpec::Filter(f) => assert_eq!(f, "name == '${HOME}'"),
        other => panic!("{other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Located errors
// ---------------------------------------------------------------------------

#[test]
fn unknown_stage_field_reports_line_stage_index_and_hint() {
    let text = "\
[source.csv]
path = \"in.csv\"

[[stages]]
filter = \"a > 1\"

[[stages]]
[stages.sort]
colums = [\"id\"]
";
    let err = parse_err(text);
    assert!(err.contains("line 9"), "{err}");
    assert!(err.contains("stage #2"), "{err}");
    assert!(err.contains("unknown field `colums`"), "{err}");
    assert!(err.contains("did you mean `columns`?"), "{err}");
}

#[test]
fn unknown_top_level_key_gets_a_hint() {
    let err = parse_err("[sorce.csv]\npath = \"x\"\n");
    assert!(err.contains("line 1"), "{err}");
    assert!(err.contains("did you mean `source`?"), "{err}");
}

#[test]
fn unknown_source_and_sink_kinds_get_hints() {
    let err = parse_err("[source.cvs]\npath = \"x\"\n");
    assert!(err.contains("unknown source"), "{err}");
    assert!(err.contains("did you mean `csv`?"), "{err}");
    let err = parse_err("[source.csv]\npath = \"x\"\n\n[sink.jsnl]\npath = \"y\"\n");
    assert!(err.contains("did you mean `jsonl`?"), "{err}");
    assert!(err.contains("line 4"), "{err}");
}

#[test]
fn typos_inside_path_only_tables_are_reported_precisely() {
    let err = parse_err("[source.csv]\npath = \"x\"\n\n[sink.csv]\npth = \"y\"\n");
    assert!(err.contains("unknown field `pth`"), "{err}");
    assert!(err.contains("did you mean `path`?"), "{err}");
    assert!(err.contains("line 5"), "{err}");
}

#[test]
fn unknown_fields_are_rejected_on_every_spec_struct() {
    for (frag, needle) in [
        ("[source.csv]\npath = \"x\"\nextra = 1\n", "extra"),
        ("[source.csv]\npath=\"x\"\n[[stages]]\naggregate = { group_by = [\"a\"], agg = {} }\n", "agg"),
        ("[source.csv]\npath=\"x\"\n[[stages]]\nexpect = { rows_at_lest = 1 }\n", "rows_at_lest"),
        ("[source.csv]\npath=\"x\"\n[[stages]]\njoin = { right = \"r\", left_keys = [\"a\"], right_keys = [\"a\"], type = \"inner\", how = 1 }\n", "how"),
        ("[source.csv]\npath=\"x\"\n[sink.json]\npath = \"o\"\npritty = true\n", "pritty"),
        ("[source.csv]\npath=\"x\"\nbogus = 1\n", "bogus"),
    ] {
        let err = parse_err(frag);
        assert!(err.contains(needle), "{frag}\n=> {err}");
        assert!(err.contains("unknown field"), "{frag}\n=> {err}");
    }
}

#[test]
fn missing_source_and_toml_syntax_errors_are_clear() {
    let err = parse_err("[[stages]]\nlimit = 3\n");
    assert!(err.contains("missing required key `source`"), "{err}");
    let err = parse_err("[source.csv\npath = 1");
    assert!(err.contains("line"), "{err}");
}

// ---------------------------------------------------------------------------
// check_spec / explain / validate / dry run
// ---------------------------------------------------------------------------

#[test]
fn check_spec_finds_semantic_problems() {
    let text = "\
error_policy = \"skp\"

[source.csv]
path = \"missing-input.csv\"
chunk_rows = 0

[[stages]]
filter = \"amount >\"

[[stages]]
aggregate = { group_by = [\"a\"], aggs = { x = \"summ\" } }

[[stages]]
join = { right = \"r.csv\", left_keys = [\"a\"], right_keys = [\"a\", \"b\"], type = \"innr\" }

[[stages]]
expect = { }

[[stages]]
sort = { columns = [] }
";
    let spec = parse_pipeline_toml_with_env(text, &no_env).unwrap();
    let diag = check_spec(&spec, false);
    let all = diag.errors.join("\n");
    assert!(all.contains("error_policy"), "{all}");
    assert!(all.contains("did you mean `skip`?"), "{all}");
    assert!(all.contains("chunk_rows"), "{all}");
    assert!(all.contains("stage #1 (filter)"), "{all}");
    assert!(all.contains("did you mean `sum`?"), "{all}");
    assert!(all.contains("did you mean `inner`?"), "{all}");
    assert!(all.contains("same length"), "{all}");
    assert!(all.contains("no checks"), "{all}");
    assert!(all.contains("stage #5 (sort)"), "{all}");
    // A missing input is only a warning unless the caller insists.
    assert!(diag
        .warnings
        .iter()
        .any(|w| w.contains("missing-input.csv")));
    let strict = check_spec(&spec, true);
    assert!(strict
        .errors
        .iter()
        .any(|e| e.contains("missing-input.csv")));
}

#[test]
fn explain_lists_source_numbered_stages_and_sink() {
    let text = "\
[source.csv]
path = \"in.csv\"

[[stages]]
filter = \"amount > 0\"

[[stages]]
sort = { columns = [\"id\"], descending = true }

[sink.csv]
path = \"out.csv\"
";
    let spec = parse_pipeline_toml_with_env(text, &no_env).unwrap();
    let plan = explain_spec(&spec);
    assert!(plan.contains("source  read CSV in.csv"), "{plan}");
    assert!(
        plan.contains("stage 1 filter: keep rows where amount > 0"),
        "{plan}"
    );
    assert!(plan.contains("stage 2 sort: by [id] descending"), "{plan}");
    assert!(plan.contains("sink    write CSV out.csv"), "{plan}");
}

#[tokio::test]
async fn validate_explain_and_dry_run_do_not_touch_data() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.csv");
    write(&input, SAMPLE);
    let pipeline = dir.path().join("p.toml");
    write(
        &pipeline,
        &format!(
            "[source.csv]\npath = \"{}\"\n\n[[stages]]\nfilter = \"amount > 0\"\n\n[sink.csv]\npath = \"{}\"\n",
            toml_path(&input),
            toml_path(&output)
        ),
    );

    let ok = validate_command(&pipeline, false).unwrap();
    assert!(ok.contains("OK (1 stage(s))"), "{ok}");
    let plan = explain_command(&pipeline, false).unwrap();
    assert!(plan.contains("stage 1 filter"), "{plan}");

    let dry = run_with(
        &pipeline,
        &RunOptions {
            quiet: true,
            dry_run: true,
            ..RunOptions::default()
        },
    )
    .await
    .unwrap();
    assert!(dry.contains("dry run: OK"), "{dry}");
    assert!(!output.exists(), "dry run must not write the sink");

    // A dry run insists that inputs exist.
    std::fs::remove_file(&input).unwrap();
    let err = run_with(
        &pipeline,
        &RunOptions {
            quiet: true,
            dry_run: true,
            ..RunOptions::default()
        },
    )
    .await
    .unwrap_err();
    assert!(format!("{err:#}").contains("does not exist"), "{err:#}");
    // ...whereas validate only warns.
    let v = validate_command(&pipeline, false).unwrap();
    assert!(v.contains("warning: source file"), "{v}");
}

#[test]
fn validate_no_env_tolerates_unset_variables() {
    let dir = tempfile::tempdir().unwrap();
    let pipeline = dir.path().join("p.toml");
    write(
        &pipeline,
        "[source.csv]\npath = \"${TPT_TEST_SURELY_UNSET_VAR}/in.csv\"\n",
    );
    assert!(validate_command(&pipeline, false).is_err());
    assert!(validate_command(&pipeline, true).is_ok());
}

// ---------------------------------------------------------------------------
// JSON schema
// ---------------------------------------------------------------------------

#[test]
fn checked_in_pipeline_schema_is_fresh() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("pipeline.schema.json");
    let generated = pipeline_schema_string();
    if std::env::var_os("TPT_UPDATE_SCHEMA").is_some() {
        std::fs::write(&path, &generated).unwrap();
    }
    let on_disk = std::fs::read_to_string(&path)
        .expect("pipeline.schema.json is missing; run `TPT_UPDATE_SCHEMA=1 cargo test -p tpt-stream-cli schema`");
    assert_eq!(
        on_disk.replace("\r\n", "\n"),
        generated,
        "pipeline.schema.json is stale; regenerate with `tptforge schema-json > \
         tpt-stream-cli/pipeline.schema.json` (or TPT_UPDATE_SCHEMA=1 cargo test -p tpt-stream-cli schema)"
    );
}

/// The `expected` field names serde reports for a struct, obtained by
/// feeding it an unknown key.
fn serde_fields(doc: &str) -> Vec<String> {
    let err = parse_err(doc);
    let start = err
        .find("expected")
        .unwrap_or_else(|| panic!("no field list in: {err}"));
    let mut names: Vec<String> = err[start..]
        .split('`')
        .enumerate()
        .filter(|(i, _)| i % 2 == 1)
        .map(|(_, t)| t.to_string())
        .collect();
    names.sort();
    names
}

fn schema_props(def: &str) -> Vec<String> {
    let defs = definitions();
    let (_, schema) = defs.iter().find(|(n, _)| *n == def).unwrap();
    let schema = if def == "pathOrTable" {
        &schema["oneOf"][1]
    } else {
        schema
    };
    let mut names: Vec<String> = schema["properties"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    names.sort();
    names
}

#[test]
fn schema_properties_match_the_serde_structs() {
    let src = "[source.csv]\npath = \"x\"\n";
    let cases: Vec<(String, &str)> = vec![
        ("[source.csv]\nbogus = 1\n".into(), "fileSource"),
        ("[source.sqlite]\nbogus = 1\n".into(), "sqliteSource"),
        ("[source.postgres]\nbogus = 1\n".into(), "connectionQuery"),
        ("[source.s3]\nbogus = 1\n".into(), "s3Object"),
        ("[source.gcs]\nbogus = 1\n".into(), "gcsObject"),
        ("[source.azure]\nbogus = 1\n".into(), "azureObject"),
        (format!("{src}[sink.csv]\nbogus = 1\n"), "pathOrTable"),
        (format!("{src}[sink.json]\nbogus = 1\n"), "jsonSink"),
        (format!("{src}[sink.columnar]\nbogus = 1\n"), "columnarSink"),
        (format!("{src}[sink.sqlite]\nbogus = 1\n"), "sqliteSink"),
        (
            format!("{src}[sink.postgres]\nbogus = 1\n"),
            "connectionTable",
        ),
        (
            format!("{src}[[stages]]\naggregate = {{ bogus = 1 }}\n"),
            "aggregate",
        ),
        (format!("{src}[[stages]]\nsort = {{ bogus = 1 }}\n"), "sort"),
        (format!("{src}[[stages]]\njoin = {{ bogus = 1 }}\n"), "join"),
        (
            format!("{src}[[stages]]\nexpect = {{ bogus = 1 }}\n"),
            "expect",
        ),
    ];
    for (doc, def) in cases {
        assert_eq!(serde_fields(&doc), schema_props(def), "definition {def}");
    }
    // Top level.
    let top = serde_fields("bogus = 1\n");
    let schema = pipeline_schema();
    let mut got: Vec<String> = schema["properties"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    got.sort();
    assert_eq!(top, got);
    // The kind lists of source / stage / sink.
    for (what, doc) in [
        ("source", "[source.zzz]\n"),
        ("stage", "[source.csv]\npath=\"x\"\n[[stages]]\nzzz = 1\n"),
        ("sink", "[source.csv]\npath=\"x\"\n[sink.zzz]\n"),
    ] {
        let err = parse_err(doc);
        let list = err
            .split("expected one of: ")
            .nth(1)
            .unwrap()
            .split(')')
            .next()
            .unwrap();
        let mut kinds: Vec<String> = list.split(", ").map(str::to_string).collect();
        kinds.sort();
        let mut schema_kinds: Vec<String> = schema["definitions"][what]["properties"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        schema_kinds.sort();
        assert_eq!(kinds, schema_kinds, "{what}");
    }
}

#[test]
fn schema_ref_targets_exist() {
    let text = pipeline_schema_string();
    let schema = pipeline_schema();
    let defs = schema["definitions"].as_object().unwrap();
    for part in text.split("\"$ref\": \"#/definitions/").skip(1) {
        let name = part.split('"').next().unwrap();
        assert!(defs.contains_key(name), "dangling $ref {name}");
    }
}

// ---------------------------------------------------------------------------
// Completions and man
// ---------------------------------------------------------------------------

#[test]
fn completions_cover_the_subcommands() {
    for shell in [
        clap_complete::Shell::Bash,
        clap_complete::Shell::Zsh,
        clap_complete::Shell::Fish,
        clap_complete::Shell::PowerShell,
    ] {
        let script = completions_command(shell);
        for cmd in [
            "run",
            "validate",
            "explain",
            "diff",
            "init",
            "doctor",
            "convert",
            "completions",
        ] {
            assert!(script.contains(cmd), "{shell:?} completions miss `{cmd}`");
        }
    }
}

#[test]
fn man_page_is_roff_and_out_dir_writes_one_page_per_command() {
    let page = man_command(None).unwrap();
    assert!(page.contains(".TH"), "{page}");
    assert!(page.contains("tptforge"), "{page}");
    let dir = tempfile::tempdir().unwrap();
    let msg = man_command(Some(dir.path())).unwrap();
    assert!(msg.contains("man page"), "{msg}");
    assert!(dir.path().join("tptforge.1").is_file());
    assert!(dir.path().join("tptforge-run.1").is_file());
    assert!(dir.path().join("tptforge-diff.1").is_file());
}

// ---------------------------------------------------------------------------
// init / doctor / convert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn init_generates_a_starter_that_parses_and_validates() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("orders.csv");
    write(&input, SAMPLE);
    let text = tools::init_toml(input.to_str().unwrap(), 1000)
        .await
        .unwrap();
    assert!(text.contains("id"), "{text}");
    assert!(text.contains("int32"), "{text}");
    assert!(text.contains("no_nulls = [\"id\", \"region\"]"), "{text}");
    // `amount` has a null in the sample, so it must not be asserted non-null.
    assert!(!text.contains("\"amount\""), "{text}");
    assert!(text.contains("# unique = [\"id\"]"), "{text}");
    let spec = parse_pipeline_toml_with_env(&text, &no_env).expect("starter parses");
    assert_eq!(spec.stages.len(), 1);
    // Wrote a file that runs.
    let p = dir.path().join("p.toml");
    write(
        &p,
        &text.replace("out.csv", &toml_path(&dir.path().join("o.csv"))),
    );
    let summary = run_command_default(&p, true).await.unwrap();
    assert!(summary.contains("3 rows"), "{summary}");
}

#[test]
fn doctor_reports_pipeline_problems() {
    let ok = tools::doctor_command(None);
    assert_eq!(ok.failures, 0, "{}", ok.text);
    assert!(ok.text.contains("all checks passed"), "{}", ok.text);

    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("p.toml");
    write(&p, "[source.csv]\npath = \"definitely-missing.csv\"\n");
    let bad = tools::doctor_command(Some(&p));
    assert!(bad.failures >= 1, "{}", bad.text);
    assert!(bad.text.contains("definitely-missing.csv"), "{}", bad.text);

    let broken = dir.path().join("b.toml");
    write(&broken, "[source.csv]\npth = 1\n");
    let bad = tools::doctor_command(Some(&broken));
    assert!(
        bad.failures >= 1 && bad.text.contains("FAIL"),
        "{}",
        bad.text
    );
}

#[tokio::test]
async fn convert_round_trips_between_formats() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("a.csv");
    write(&csv, SAMPLE);
    let jsonl = dir.path().join("b.jsonl");
    let col = dir.path().join("c.tptcol");
    let json = dir.path().join("d.json");
    let back = dir.path().join("e.csv");
    let m = tools::convert_command(csv.to_str().unwrap(), &jsonl, "strict", false)
        .await
        .unwrap();
    assert!(m.contains("3 row"), "{m}");
    let lines = std::fs::read_to_string(&jsonl).unwrap();
    assert_eq!(lines.lines().count(), 3, "{lines}");
    assert!(lines.contains("\"north\""), "{lines}");
    tools::convert_command(csv.to_str().unwrap(), &json, "strict", false)
        .await
        .unwrap();
    assert!(std::fs::read_to_string(&json)
        .unwrap()
        .trim_start()
        .starts_with('['));
    // Columnar (zstd) keeps schema and order exactly.
    tools::convert_command(csv.to_str().unwrap(), &col, "strict", true)
        .await
        .unwrap();
    tools::convert_command(col.to_str().unwrap(), &back, "strict", false)
        .await
        .unwrap();
    let text = std::fs::read_to_string(&back).unwrap();
    assert_eq!(
        text,
        "id,region,amount
1,north,5
2,south,
3,north,7
"
    );
    let err = tools::convert_command(
        csv.to_str().unwrap(),
        &dir.path().join("x.xyz"),
        "strict",
        false,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("cannot infer the output format"),
        "{err}"
    );
}

// ---------------------------------------------------------------------------
// diff
// ---------------------------------------------------------------------------

async fn diff(a: &str, b: &str, keys: &[&str]) -> anyhow::Result<(String, tools::DiffSummary)> {
    let dir = tempfile::tempdir().unwrap();
    let pa = dir.path().join("a.csv");
    let pb = dir.path().join("b.csv");
    write(&pa, a);
    write(&pb, b);
    let keys: Vec<String> = keys.iter().map(|s| s.to_string()).collect();
    let mut out = Vec::new();
    let summary =
        tools::diff_command(pa.to_str().unwrap(), pb.to_str().unwrap(), &keys, &mut out).await?;
    Ok((String::from_utf8(out).unwrap(), summary))
}

#[tokio::test]
async fn diff_reports_added_removed_and_changed_rows() {
    let a = "id,name,score\n1,ann,10\n2,bob,20\n3,cy,30\n5,eve,50\n";
    // Deliberately shuffled: diff sorts by key itself.
    let b = "id,name,score\n5,eve,50\n4,dee,40\n2,bob,21\n1,ann,10\n";
    let (out, s) = diff(a, b, &["id"]).await.unwrap();
    assert_eq!(
        out,
        "_diff,id,name,score\n-,2,bob,20\n+,2,bob,21\n-,3,cy,30\n+,4,dee,40\n"
    );
    assert_eq!((s.added, s.removed, s.changed, s.unchanged), (1, 1, 1, 2));
    assert!(s.differs());
    assert!(s
        .to_string()
        .contains("1 added, 1 removed, 1 changed, 2 unchanged"));
}

#[tokio::test]
async fn diff_of_identical_files_is_empty() {
    let a = "id,v\n1,x\n2,y\n";
    let (out, s) = diff(a, a, &["id"]).await.unwrap();
    assert_eq!(out, "_diff,id,v\n");
    assert!(!s.differs());
    assert_eq!(s.unchanged, 2);
}

#[tokio::test]
async fn diff_supports_composite_keys_and_string_keys() {
    let a = "region,day,n\nn,1,5\nn,2,6\ns,1,7\n";
    let b = "region,day,n\nn,2,6\ns,1,8\ns,2,9\n";
    let (out, s) = diff(a, b, &["region", "day"]).await.unwrap();
    assert_eq!(
        out,
        "_diff,region,day,n\n-,n,1,5\n-,s,1,7\n+,s,1,8\n+,s,2,9\n"
    );
    assert_eq!((s.added, s.removed, s.changed, s.unchanged), (1, 1, 1, 1));
}

#[tokio::test]
async fn diff_handles_empty_sides_and_quotes_fields() {
    let (out, s) = diff("id,v\n", "id,v\n1,\"a,b\"\n", &["id"]).await.unwrap();
    assert_eq!(out, "_diff,id,v\n+,1,\"a,b\"\n");
    assert_eq!(s.added, 1);
    let (out, s) = diff("id,v\n1,z\n", "id,v\n", &["id"]).await.unwrap();
    assert_eq!(out, "_diff,id,v\n-,1,z\n");
    assert_eq!(s.removed, 1);
}

#[tokio::test]
async fn diff_rejects_duplicate_keys_missing_keys_and_type_mismatch() {
    let err = diff("id,v\n1,a\n1,b\n", "id,v\n1,a\n", &["id"])
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("duplicate key (1)"), "{err:#}");

    let err = diff("id,v\n1,a\n", "id,v\n1,a\n", &["nope"])
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("nope"), "{err:#}");

    let err = diff("id,v\n1,a\n", "id,v\nx,a\n", &["id"])
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("int32 in"), "{err:#}");
}

#[tokio::test]
async fn diff_notes_columns_present_on_one_side_only() {
    let (out, s) = diff("id,v,old\n1,a,x\n", "id,v,new\n1,a,y\n", &["id"])
        .await
        .unwrap();
    assert_eq!(out, "_diff,id,v\n");
    assert!(s.notes.iter().any(|n| n.contains("old")), "{:?}", s.notes);
    assert!(s.notes.iter().any(|n| n.contains("new")), "{:?}", s.notes);
    assert!(!s.differs());
}

// ---------------------------------------------------------------------------
// schema drift
// ---------------------------------------------------------------------------

#[tokio::test]
async fn schema_save_then_against_detects_drift() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("d.csv");
    let saved = dir.path().join("schema.json");
    write(&data, "id,name,score\n1,a,1.5\n");
    let msg = tools::schema_save(data.to_str().unwrap(), 100, &saved)
        .await
        .unwrap();
    assert!(msg.contains("3 column"), "{msg}");
    let ok = tools::schema_against(data.to_str().unwrap(), 100, &saved)
        .await
        .unwrap();
    assert!(!ok.drifted, "{}", ok.report);

    // Type change + removed + added.
    write(&data, "id,score,extra\n1,abc,true\n");
    let d = tools::schema_against(data.to_str().unwrap(), 100, &saved)
        .await
        .unwrap();
    assert!(d.drifted);
    assert!(
        d.report.contains("- column removed: name (string)"),
        "{}",
        d.report
    );
    assert!(
        d.report
            .contains("~ column type changed: score: float64 -> string"),
        "{}",
        d.report
    );
    assert!(
        d.report.contains("+ column added: extra (bool)"),
        "{}",
        d.report
    );

    // Reordering alone is drift too.
    write(&data, "name,id,score\na,1,1.5\n");
    let d = tools::schema_against(data.to_str().unwrap(), 100, &saved)
        .await
        .unwrap();
    assert!(
        d.drifted && d.report.contains("column order changed"),
        "{}",
        d.report
    );
}

#[tokio::test]
async fn schema_against_a_garbage_file_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("d.csv");
    let bad = dir.path().join("bad.json");
    write(&data, "a\n1\n");
    write(&bad, "not json");
    assert!(tools::schema_against(data.to_str().unwrap(), 10, &bad)
        .await
        .is_err());
}

// ---------------------------------------------------------------------------
// manifest
// ---------------------------------------------------------------------------

#[test]
fn sha256_matches_known_vectors() {
    assert_eq!(
        sha256_hex(b"abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(
        sha256_hex(b""),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}

#[test]
fn rfc3339_formats_unix_times() {
    assert_eq!(rfc3339_utc(0), "1970-01-01T00:00:00Z");
    assert_eq!(rfc3339_utc(1_000_000_000), "2001-09-09T01:46:40Z");
}

#[tokio::test]
async fn manifest_records_counts_stages_and_hashes() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.csv");
    let manifest = dir.path().join("m.json");
    write(&input, SAMPLE);
    let pipeline = dir.path().join("p.toml");
    let text = format!(
        "[source.csv]\npath = \"{}\"\n\n[[stages]]\nfilter = \"id > 1\"\n\n[sink.csv]\npath = \"{}\"\n",
        toml_path(&input),
        toml_path(&output)
    );
    write(&pipeline, &text);
    run_with(
        &pipeline,
        &RunOptions {
            quiet: true,
            manifest: Some(manifest.clone()),
            ..RunOptions::default()
        },
    )
    .await
    .unwrap();

    let m: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest).unwrap()).unwrap();
    assert_eq!(m["manifest_version"], 1);
    assert_eq!(m["status"], "ok");
    assert_eq!(m["spec_sha256"], sha256_hex(text.as_bytes()));
    assert_eq!(m["inputs"][0]["sha256"], sha256_hex(SAMPLE.as_bytes()));
    assert_eq!(m["inputs"][0]["bytes"], SAMPLE.len());
    let out_bytes = std::fs::read(&output).unwrap();
    assert_eq!(m["outputs"][0]["sha256"], sha256_hex(&out_bytes));
    assert_eq!(m["stats"]["rows"], 3);
    assert_eq!(m["stages"][0]["index"], 1);
    assert_eq!(m["stages"][0]["rows_in"], 3);
    assert_eq!(m["stages"][0]["rows_out"], 2);
    assert!(m["started_at"].as_str().unwrap().ends_with('Z'));
}

#[tokio::test]
async fn manifest_is_written_for_failed_runs_too() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let manifest = dir.path().join("m.json");
    write(&input, SAMPLE);
    let pipeline = dir.path().join("p.toml");
    write(
        &pipeline,
        &format!(
            "[source.csv]\npath = \"{}\"\n\n[[stages]]\nexpect = {{ rows_at_least = 99 }}\n",
            toml_path(&input)
        ),
    );
    let result = run_with(
        &pipeline,
        &RunOptions {
            quiet: true,
            manifest: Some(manifest.clone()),
            ..RunOptions::default()
        },
    )
    .await;
    assert!(result.is_err());
    let m: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest).unwrap()).unwrap();
    assert_eq!(m["status"], "failed");
    assert!(m["error"].as_str().unwrap().contains("data quality"), "{m}");
}

// ---------------------------------------------------------------------------
// watch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn watch_reruns_when_the_input_changes() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.csv");
    write(&input, "id\n1\n");
    let pipeline = dir.path().join("p.toml");
    write(
        &pipeline,
        &format!(
            "[source.csv]\npath = \"{}\"\n\n[sink.csv]\npath = \"{}\"\n",
            toml_path(&input),
            toml_path(&output)
        ),
    );
    let options = RunOptions {
        quiet: true,
        ..RunOptions::default()
    };
    let changer = {
        let input = input.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(400)).await;
            std::fs::write(&input, "id\n1\n2\n3\n").unwrap();
        })
    };
    tokio::time::timeout(
        Duration::from_secs(30),
        tools::watch_command(&pipeline, &options, Duration::from_millis(50), Some(2)),
    )
    .await
    .expect("watch timed out")
    .unwrap();
    changer.await.unwrap();
    let out = std::fs::read_to_string(&output).unwrap();
    assert_eq!(
        out.lines().count(),
        4,
        "second run must see the new input: {out}"
    );
}

#[tokio::test]
async fn watch_rejects_metrics() {
    let options = RunOptions {
        metrics: Some("127.0.0.1:0".into()),
        ..RunOptions::default()
    };
    let err = tools::watch_command(
        Path::new("x.toml"),
        &options,
        Duration::from_millis(10),
        Some(1),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("--metrics"), "{err}");
}
