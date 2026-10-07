"""Submit a fixture-only partition through PostgreSQL queue, daemon and gRPC launcher."""

import argparse
import json
import time
import urllib.request


def graphql(url, query, variables=None):
    request = urllib.request.Request(
        f"{url}/graphql", data=json.dumps({"query": query, "variables": variables or {}}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.load(response)
    if payload.get("errors"):
        raise RuntimeError(payload["errors"])
    return payload["data"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:13000")
    parser.add_argument(
        "--db-url",
        default=None,
        help="PostgreSQL connection string to verify metadata persistence.",
    )
    args = parser.parse_args()
    repositories = graphql(args.url, """{
      repositoriesOrError { __typename ... on RepositoryConnection {
        nodes { name location { name } schedules { name scheduleState { status } } }
      } }
    }""")["repositoriesOrError"]
    assert repositories["__typename"] == "RepositoryConnection", repositories
    repo = next(r for r in repositories["nodes"] if r["location"]["name"] == "procurement")
    assert all(s["scheduleState"]["status"] == "STOPPED" for s in repo["schedules"]), repo
    params = {
        "selector": {"repositoryName": repo["name"], "repositoryLocationName": "procurement",
                     "pipelineName": "bronze_daily"},
        "runConfigData": {}, "mode": "default",
        "executionMetadata": {"tags": [{"key": "dagster/partition", "value": "2025-01-01"}]},
    }
    launched = graphql(args.url, """mutation($params: ExecutionParams!) {
      launchRun(executionParams: $params) {
        __typename ... on LaunchRunSuccess { run { runId } }
        ... on PythonError { message }
        ... on RunConfigValidationInvalid { errors { message } }
      }
    }""", {"params": params})["launchRun"]
    assert launched["__typename"] == "LaunchRunSuccess", launched
    run_id = launched["run"]["runId"]
    print(f"Submitted {run_id}", flush=True)
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        run = graphql(args.url, """query($id: ID!) {
          runOrError(runId: $id) { __typename ... on Run { status } }
        }""", {"id": run_id})["runOrError"]
        if run.get("status") in {"SUCCESS", "FAILURE", "CANCELED"}:
            break
        time.sleep(1)
    assert run.get("status") == "SUCCESS", run
    logs = graphql(args.url, """query($id: ID!) {
      logsForRun(runId: $id) { ... on EventConnection { events { __typename } } }
    }""", {"id": run_id})["logsForRun"]["events"]
    assert sum(e["__typename"] == "MaterializationEvent" for e in logs) == 5, logs
    assert sum(e["__typename"] == "AssetCheckEvaluationEvent" for e in logs) == 12, logs
    print(
        "SUCCESS: five materializations and twelve checks through the queued launcher",
        flush=True,
    )

    db_url = args.db_url
    if db_url:
        import sqlalchemy as sa

        engine = sa.create_engine(db_url)
        with engine.connect() as conn:
            rows = conn.execute(
                sa.text(
                    "SELECT count(*) FROM bronze_meta.attempts WHERE dagster_run_id = :run_id AND status = 'success'"
                ),
                {"run_id": run_id},
            ).scalar()
            assert rows == 5, (
                f"Expected 5 success attempts in DB for dagster_run_id {run_id}, got {rows}"
            )
        print(
            f"SUCCESS: verified 5 PostgreSQL attempts correlated with Dagster run {run_id}",
            flush=True,
        )


if __name__ == "__main__":
    main()
