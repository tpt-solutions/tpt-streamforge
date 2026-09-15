/*
 * tpt-streamforge C ABI integration smoke test.
 *
 * Build:
 *   cargo build -p tpt-stream-ffi
 *   gcc tests/c_integration/main.c -I include -o ctest.exe target/debug/tpt_stream_ffi.dll
 *   ./ctest.exe
 */
#include "tpt_streamforge.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FAIL(...)                                          \
    do {                                                   \
        fprintf(stderr, "FAIL at %s:%d: ", __FILE__, __LINE__); \
        fprintf(stderr, __VA_ARGS__);                      \
        fprintf(stderr, "\n");                             \
        return 1;                                          \
    } while (0)

static void build_path(char *buf, size_t cap, const char *name) {
    const char *tmp = getenv("TEMP");
    if (tmp == NULL) tmp = getenv("TMP");
    if (tmp == NULL) tmp = ".";
    snprintf(buf, cap, "%s\\tpt-c-%s", tmp, name);
}

static int write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f == NULL) return -1;
    fputs(content, f);
    fclose(f);
    return 0;
}

static int require_ok(int rc, void *pipeline) {
    if (rc == TPT_OK) return 0;
    char err[512];
    size_t written = 0;
    tpt_pipeline_free(pipeline);
    tpt_error_string(err, sizeof(err), &written);
    FAIL("non-OK status %d: %s", rc, written ? err : "(no detail)");
}

static int run_suite(void) {
    char input[512], output[512];
    build_path(input, sizeof(input), "input.csv");
    build_path(output, sizeof(output), "output.csv");

    /* ---- filter + map + write ---- */
    if (write_file(input, "id,name,score\n1,alice,90\n2,bob,40\n3,carol,72\n") != 0) {
        FAIL("cannot write input");
    }

    void *p = NULL;
    if (require_ok(tpt_pipeline_new(&p), p)) return 1;

    if (require_ok(tpt_pipeline_read_csv(p, input, 0), p)) return 1;
    if (require_ok(tpt_pipeline_filter(p, "score > 60 and length(name) > 3"), p)) return 1;

    const char *cols[] = {"id", "label"};
    const char *exprs[] = {"id", "upper(name)"};
    if (require_ok(tpt_pipeline_map(p, cols, exprs, 2), p)) return 1;

    if (require_ok(tpt_pipeline_write_csv(p, output), p)) return 1;

    uint64_t rows = 0;
    if (require_ok(tpt_pipeline_execute(p, &rows), p)) return 1;
    if (rows != 3) FAIL("expected 3 source rows, got %llu", (unsigned long long)rows);
    tpt_pipeline_free(p);

    FILE *f = fopen(output, "r");
    if (f == NULL) FAIL("output not found");
    char line[256];
    const char *expected = "id,label\n1,ALICE\n3,CAROL\n";
    size_t got_len = 0;
    while (fgets(line, sizeof(line), f) != NULL) {
        size_t l = strlen(line);
        if (got_len + l > strlen(expected)) FAIL("output too long");
        if (strncmp(expected + got_len, line, l) != 0) FAIL("unexpected line: %s", line);
        got_len += l;
    }
    fclose(f);
    if (got_len != strlen(expected)) FAIL("output truncated");

    /* ---- expression error path ---- */
    p = NULL;
    if (require_ok(tpt_pipeline_new(&p), p)) return 1;
    if (require_ok(tpt_pipeline_read_csv(p, input, 0), p)) return 1;
    if (require_ok(tpt_pipeline_filter(p, "a >"), p)) return 1;
    if (require_ok(tpt_pipeline_write_csv(p, output), p)) return 1;
    if (tpt_pipeline_execute(p, NULL) != TPT_ERR_SCHEMA) {
        FAIL("expected TPT_ERR_SCHEMA for bad expression");
    }
    {
        char err[512];
        size_t written = 0;
        if (tpt_error_string(err, sizeof(err), &written) != TPT_OK || written == 0) {
            FAIL("expected a last-error string");
        }
        if (strstr(err, "expression") == NULL) FAIL("unexpected last-error: %s", err);
    }
    tpt_pipeline_free(p);

    /* ---- aggregate + sort ---- */
    if (write_file(input, "k,v\n0,10\n1,5\n0,7\n2,1\n2,2\n1,3\n") != 0) {
        FAIL("cannot write input");
    }
    p = NULL;
    if (require_ok(tpt_pipeline_new(&p), p)) return 1;
    if (require_ok(tpt_pipeline_read_csv(p, input, 0), p)) return 1;

    const char *gb[] = {"k"};
    struct TptAggSpec spec;
    spec.func = TPT_AGG_SUM;
    spec.column = "v";
    spec.output = "total";
    if (require_ok(tpt_pipeline_aggregate(p, gb, 1, &spec, 1), p)) return 1;

    const char *sort_cols[] = {"total"};
    if (require_ok(tpt_pipeline_sort(p, sort_cols, 1, 0), p)) return 1;

    if (require_ok(tpt_pipeline_write_csv(p, output), p)) return 1;
    if (require_ok(tpt_pipeline_execute(p, NULL), p)) return 1;

    f = fopen(output, "r");
    if (f == NULL) FAIL("agg output not found");
    char *got[3] = {0};
    size_t n = 0;
    fgets(line, sizeof(line), f); /* header */
    while (fgets(line, sizeof(line), f) != NULL && n < 3) {
        got[n] = strdup(line);
        n++;
    }
    fclose(f);
    if (n != 3) FAIL("expected 3 agg rows (sort asc), got %zu", n);
    /* groups 0(17), 1(8), 2(3) ascending -> 2,1,0 */
    const char *expected_groups[] = {"k,total\n", "2,3\n", "1,8\n", "0,17\n"};
    for (size_t i = 0; i < 4; i++) {
        const char *actual = i == 0 ? "k,total\n" : got[i - 1];
        if (strcmp(actual, expected_groups[i]) != 0) {
            FAIL("agg row %zu: got \"%s\" want \"%s\"", i, actual, expected_groups[i]);
        }
    }
    for (size_t i = 0; i < 3; i++) free(got[i]);
    tpt_pipeline_free(p);

    puts("c_integration: all OK");
    return 0;
}

int main(void) {
    return run_suite();
}