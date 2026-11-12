import argparse
import json
import sys
from .service import Service
from .values import strict_json, canonical, require_object
from .errors import MetrologyError, InputError
from .ingress.readers import load_text, load_bytes, read_input, summarize
from .ingress.mapping import Mapping
from .ingress.delivery import DeliveryInbox
from .output.snapshots import verify_export, atomic_write
from .processing.diagnostics import Diagnostics
from .calibration.fitting import fit_linear, leave_one_out
from .ledger.audit import history
from .demo import demonstration


def document(path):
    return strict_json(load_text(path))


def dispatch(app, request):
    require_object(request, required={"action"})
    action = request["action"]
    options = dict(request)
    del options["action"]
    if action == "configure":
        return app.configuration.apply(options["configuration"])
    if action == "catalog":
        return app.catalog.inventory()
    if action == "export_config":
        return app.configuration.export(options["id"])
    if action == "readiness":
        return app.configuration.readiness(**options)
    if action == "ingest":
        mapping = Mapping.parse(options["mapping"]) if options.get("mapping") else None
        format_name = options.get("format", "jsonl")
        parse_options = (
            {"delimiter": options.get("delimiter", ",")} if format_name == "csv" else {}
        )
        rows = read_input(
            load_text(options["file"]), format_name, mapping, **parse_options
        )
        return app.pipeline.ingest(
            options["batch"],
            rows,
            received_at=options["received_at"],
            atomic=options.get("atomic", False),
        )
    if action == "receive":
        return DeliveryInbox(app.database, app.pipeline).receive(
            options["manifest"],
            options["inbox"],
            maximum_files=options.get("maximum_files"),
        )
    if action == "delivery":
        return DeliveryInbox(app.database, app.pipeline).status(options["id"])
    if action == "pending_deliveries":
        return DeliveryInbox(app.database, app.pipeline).pending()
    if action == "observation":
        return app.pipeline.get(options["id"], options.get("revision"))
    if action == "history":
        return app.pipeline.history(options["id"])
    if action == "batch":
        return app.reports.batch(options["id"])
    if action == "query":
        return app.reports.query(**options)
    if action == "quality":
        return app.reports.quality(**options)
    if action == "sequence_gaps":
        return app.reports.sequence_gaps(options["source"])
    if action == "watermark":
        return {"changed": app.catalog.watermark(options["source"], options["at"])}
    if action == "revoke":
        return {
            "changed": app.catalog.revoke_certificate(
                options["certificate"], options["reason"]
            )
        }
    if action == "impact":
        return app.reports.certificate_impact(options["certificate"])
    if action == "bind":
        return {
            "changed": app.catalog.bind(
                options["instrument"], options["channel"], options["policy"]
            )
        }
    if action == "instrument_state":
        return {
            "changed": app.catalog.set_instrument_active(
                options["instrument"], options["active"]
            )
        }
    if action == "replay":
        return app.pipeline.replay(**options)
    if action == "windows":
        return app.windows.materialize(
            options["specification"], options.get("expected_generation")
        )
    if action == "window":
        return app.windows.get(options["id"])
    if action == "seal":
        return app.windows.seal(options["id"], options["generation"])
    if action == "snapshot":
        return app.snapshots.create(options["id"], options["windows"])
    if action == "export":
        return app.snapshots.export(options["id"], options["destination"])
    if action == "export_csv":
        return atomic_write(
            options["destination"], app.snapshots.csv(options["id"]).encode("utf-8")
        )
    if action == "compare":
        return app.snapshots.compare(options["left"], options["right"])
    if action == "profile":
        return Diagnostics(app.reports).profile(**options)
    if action == "trend":
        return Diagnostics(app.reports).trend(**options)
    if action == "audit":
        return history(app.database, **options)
    if action == "check":
        return app.inspector.check()
    if action == "stale":
        return app.inspector.stale_windows()
    if action == "manifest":
        return app.inspector.manifest()
    if action == "compare_manifest":
        return app.inspector.compare_manifest(options["manifest"])
    if action == "backup":
        return {"path": app.database.backup(options["destination"])}
    if action == "overview":
        return app.reports.overview()
    if action == "campaign_plan":
        return app.campaigns.plan(options["specification"])
    if action == "campaign_start":
        return app.campaigns.start(options["id"], options["revision"])
    if action == "campaign_assess":
        return app.campaigns.assess(options["id"])
    if action == "campaign_submit":
        return app.campaigns.submit(options["id"], options["revision"])
    if action == "campaign_decide":
        return app.campaigns.decide(
            options["id"],
            options["revision"],
            options["reviewer"],
            options["approved"],
            options["reason"],
        )
    if action == "campaign":
        return app.campaigns.get(options["id"])
    if action == "campaign_decisions":
        return app.campaigns.decisions(options["id"])
    if action == "bundle":
        return app.bundles.export(
            options["campaign"], options["snapshot"], options["destination"]
        )
    if action == "verify_bundle":
        from .output.bundles import verify_bundle

        return verify_bundle(options["file"])
    if action == "replay_plan":
        return app.replay_plans.plan(**options)
    if action == "replay_execute":
        return app.replay_plans.execute(options["id"])
    if action == "replay_status":
        return app.replay_plans.get(options["id"])
    if action == "gateway_directory":
        document = options["document"]
        if isinstance(document, str):
            document = strict_json(load_text(document))
        return {"registered": app.directories.register(document)}
    if action == "gateway_directories":
        return {"directories": app.directories.listing()}
    if action == "gateway_collect":
        collect_options = {}
        for key in ("received_at", "chunk_size", "timeout"):
            if key in options:
                collect_options[key] = options[key]
        return app.gateway.collect_http(
            options["session"], options["gateway"], options["url"], **collect_options
        )
    if action == "gateway_session":
        return app.gateway.status(options["id"])
    if action == "gateway_events":
        return {"events": app.gateway.events(options["id"])}
    if action == "gateway_frames":
        return {
            "frames": app.gateway.frames(
                options["id"], status=options.get("status")
            )
        }
    if action == "gateway_reprocess":
        return app.gateway.reprocess_session(
            options["session"],
            batch_id=options.get("batch"),
            received_at=options.get("received_at"),
        )
    if action == "gateway_reprocess_frame":
        raw = load_bytes(options["file"])
        return app.gateway.reprocess_frame(
            options["gateway"],
            raw,
            batch_id=options["batch"],
            received_at=options["received_at"],
        )
    if action == "crosscheck":
        from .processing.crosscheck import PairedChannels

        return PairedChannels(app.reports, app.catalog.registry()).compare(**options)
    raise InputError("unknown action", field=action)


def parser():
    root = argparse.ArgumentParser(
        prog="metrology", description="Traceable instrument data processing"
    )
    root.add_argument("--db", default="metrology.sqlite3")
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("demo", help="execute local synthetic end-to-end acceptance")
    commands.add_parser("init", help="initialize and inspect database")
    run = commands.add_parser("run", help="execute one JSON action document")
    run.add_argument("file")
    preview = commands.add_parser("preview", help="parse observations without writing")
    preview.add_argument("file")
    preview.add_argument("--format", choices=["csv", "jsonl"], default="jsonl")
    preview.add_argument("--mapping")
    preview.add_argument("--delimiter", default=",")
    verify = commands.add_parser("verify-export", help="verify standalone snapshot")
    verify.add_argument("file")
    fit = commands.add_parser("fit", help="fit reference pairs to a calibration")
    fit.add_argument("file")
    fit.add_argument("--residual-limit")
    fit.add_argument("--leave-one-out", action="store_true")
    commands.add_parser("check", help="verify stored data and audit chain")
    commands.add_parser("overview", help="count stored business objects")
    return root


def execute(args):
    if args.command == "demo":
        return demonstration()
    if args.command == "verify-export":
        return verify_export(args.file)
    if args.command == "fit":
        data = document(args.file)
        return (
            leave_one_out(data)
            if args.leave_one_out
            else fit_linear(data, residual_limit=args.residual_limit)
        )
    if args.command == "preview":
        mapping = Mapping.parse(document(args.mapping)) if args.mapping else None
        options = {"delimiter": args.delimiter} if args.format == "csv" else {}
        return summarize(
            read_input(load_text(args.file), args.format, mapping, **options)
        )
    with Service(args.db) as app:
        if args.command in {"init", "check"}:
            return app.inspector.check()
        if args.command == "overview":
            return app.reports.overview()
        if args.command == "run":
            return dispatch(app, document(args.file))
        raise InputError("unknown command")


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        result = execute(args)
    except MetrologyError as exc:
        print(canonical({"error": exc.as_dict()}), file=sys.stderr)
        return 2
    except (KeyError, TypeError) as exc:
        print(
            canonical({"error": {"code": "invalid_request", "message": str(exc)}}),
            file=sys.stderr,
        )
        return 2
    except (OSError, UnicodeError) as exc:
        print(
            canonical({"error": {"code": "io_error", "message": str(exc)}}),
            file=sys.stderr,
        )
        return 3
    print(
        json.dumps(
            result, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False
        )
    )
    return 0
