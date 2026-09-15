"""tpt-streamforge: a streaming ETL engine.

The heavy lifting happens in the Rust `_native` module (PyO3); this package
re-exports the `Pipeline` class with a clean public API and constants.

Example::

    from tpt_streamforge import Pipeline

    stats = (
        Pipeline()
        .read_csv("in.csv")
        .filter("score > 60")
        .map({"label": "upper(name)"})
        .group_by(["score"])
        .agg({"label": "count"})
        .write_csv("out.csv")
        .execute()
    )
"""

from ._native import GroupBy, Pipeline, TptError  # noqa: F401

__all__ = ["Pipeline", "GroupBy", "TptError"]
__version__ = "0.1.0"