"""``kterm watchlist ...`` and ``kterm alerts ...`` (the parts that talk REST).

Writes go through the library's calls that are never retried. When a command works on several
things (tickers, rule ids) a problem with one is reported and the rest still run; the exit status
is 1 if any failed.
"""

from typing import Annotated, Any

import typer

from kalshiterm_client import views
from kalshiterm_client.cli_common import emit_json, fail, make_console, run_with_api
from kalshiterm_client.client import ServerClient, decimal_text
from kalshiterm_client.errors import ApiProblem, NotFound

JsonOption = Annotated[bool, typer.Option("--json", help="Machine-readable output.")]

HINTS = {
    "not_watched": "watch it first: kterm watchlist add {ticker}",
    "watchlist_full": "remove something first: kterm watchlist remove TICKER",
}


def explain(exc: ApiProblem) -> str:
    """The server's refusal, with what to do about it where we know."""
    code, extra = exc.code, exc.extra
    if code == "invalid_comparison":
        return f"{exc} (allowed for this metric: {', '.join(extra.get('allowed', []))})"
    if code == "cooldown_too_short":
        return f"{exc} (at least {extra.get('minimum')} seconds)"
    if code == "rule_limit":
        return f"{exc} (you can have {extra.get('limit')} rules; remove one with kterm alerts rm)"
    if code in HINTS and "ticker" in extra:
        return f"{exc}; {HINTS[code].format(ticker=extra['ticker'])}"
    return str(exc)


def register(app: typer.Typer) -> typer.Typer:
    """Adds ``watchlist`` and ``alerts``; returns the alerts group for the live commands."""
    watchlist_app = typer.Typer(
        help="Markets you want captured and analysed.", invoke_without_command=True
    )
    alerts_app = typer.Typer(
        help="Alert rules and the alerts they raise.", invoke_without_command=True
    )
    app.add_typer(watchlist_app, name="watchlist")
    app.add_typer(alerts_app, name="alerts")

    # ---------------------------------------------------------------- watchlist

    def show_watchlist(as_json: bool) -> None:
        listing = run_with_api(lambda api: api.watchlist())
        if as_json:
            return emit_json(listing)
        make_console().print(views.watchlist_table(listing))

    @watchlist_app.callback()
    def watchlist_default(ctx: typer.Context) -> None:
        if ctx.invoked_subcommand is None:
            show_watchlist(False)

    @watchlist_app.command("list")
    def watchlist_list(as_json: JsonOption = False) -> None:
        """Your watchlist."""
        show_watchlist(as_json)

    def change_watchlist(tickers: list[str], adding: bool) -> None:
        async def work(api: ServerClient) -> list[tuple[str, bool | None, ApiProblem | None]]:
            done: list[tuple[str, bool | None, ApiProblem | None]] = []
            for ticker in tickers:
                try:
                    changed = await (api.watch(ticker) if adding else api.unwatch(ticker))
                    done.append((ticker, changed, None))
                except ApiProblem as exc:
                    done.append((ticker, None, exc))
            return done

        problems = 0
        for ticker, changed, exc in run_with_api(work):
            if exc is not None:
                problems += 1
                typer.echo(f"{ticker}: {explain(exc)}", err=True)
            elif adding:
                typer.echo(f"{'added' if changed else 'already on your list'}: {ticker}")
            else:
                typer.echo(f"{'removed' if changed else 'was not on your list'}: {ticker}")
        if problems:
            raise typer.Exit(code=1)

    @watchlist_app.command("add")
    def watchlist_add(
        tickers: Annotated[list[str], typer.Argument(help="Market tickers.")],
    ) -> None:
        """Watch markets: the server captures their orderbooks and analyses them."""
        change_watchlist(tickers, adding=True)

    @watchlist_app.command("remove")
    def watchlist_remove(
        tickers: Annotated[list[str], typer.Argument(help="Market tickers.")],
    ) -> None:
        """Stop watching markets (the server keeps capturing them for at least 12 hours)."""
        change_watchlist(tickers, adding=False)

    # ---------------------------------------------------------------- alerts

    def show_alerts(unacknowledged: bool, limit: int, as_json: bool) -> None:
        async def work(api: ServerClient) -> list[Any]:
            found: list[Any] = []
            async for alert in api.iter_alerts(
                unacknowledged=unacknowledged, limit=min(limit, 200)
            ):
                found.append(alert)
                if len(found) >= limit:
                    break
            return found

        found = run_with_api(work)
        if as_json:
            return emit_json({"alerts": found})
        if not found:
            typer.echo("no alerts" if not unacknowledged else "no unacknowledged alerts")
            return
        make_console().print(views.alerts_table(found))

    @alerts_app.callback()
    def alerts_default(ctx: typer.Context) -> None:
        if ctx.invoked_subcommand is None:
            show_alerts(False, 20, False)

    @alerts_app.command("list")
    def alerts_list(
        unacknowledged: Annotated[bool, typer.Option("--unacknowledged", "-u")] = False,
        limit: Annotated[int, typer.Option(min=1, max=500)] = 20,
        as_json: JsonOption = False,
    ) -> None:
        """Your alerts, newest first."""
        show_alerts(unacknowledged, limit, as_json)

    @alerts_app.command("ack")
    def alerts_ack(
        ids: Annotated[list[int] | None, typer.Argument(help="Alert ids.")] = None,
        all_: Annotated[bool, typer.Option("--all", help="Every unacknowledged alert.")] = False,
    ) -> None:
        """Acknowledge alerts (it is harmless to do it twice)."""
        if bool(ids) == all_:
            raise fail("give alert ids, or --all")

        async def work(api: ServerClient) -> list[tuple[int, ApiProblem | None]]:
            targets = ids or [a.id async for a in api.iter_alerts(unacknowledged=True)]
            done: list[tuple[int, ApiProblem | None]] = []
            for alert_id in targets:
                try:
                    await api.acknowledge_alert(alert_id)
                    done.append((alert_id, None))
                except ApiProblem as exc:
                    done.append((alert_id, exc))
            return done

        results = run_with_api(work)
        for alert_id, exc in results:
            if exc is not None:
                typer.echo(f"alert {alert_id}: {explain(exc)}", err=True)
        acknowledged = sum(1 for _, exc in results if exc is None)
        typer.echo(f"acknowledged {acknowledged} alert(s)")
        if acknowledged != len(results):
            raise typer.Exit(code=1)

    @alerts_app.command("metrics")
    def alerts_metrics(as_json: JsonOption = False) -> None:
        """What alert rules can be set on."""
        catalog = run_with_api(lambda api: api.alert_metrics())
        if as_json:
            return emit_json(catalog)
        make_console().print(views.metrics_table(catalog.metrics))

    @alerts_app.command("rules")
    def alerts_rules(as_json: JsonOption = False) -> None:
        """Your alert rules."""
        rules = run_with_api(lambda api: api.alert_rules())
        if as_json:
            return emit_json(rules)
        make_console().print(views.rules_table(rules.rules, rules.limit))

    @alerts_app.command("add")
    def alerts_add(
        metric: Annotated[str, typer.Argument(help="analyzer.metric, see: kterm alerts metrics")],
        subject: Annotated[str, typer.Argument(help="The market ticker (or event ticker).")],
        op: Annotated[str, typer.Argument(help="gt, lt or abs_gt (either direction).")],
        threshold: Annotated[str, typer.Argument(help="In the metric's unit, e.g. 0.05.")],
        cooldown: Annotated[
            int | None, typer.Option(help="Seconds between alerts (default 900).")
        ] = None,
        name: Annotated[str, typer.Option(help="A label for the alert.")] = "",
        as_json: JsonOption = False,
    ) -> None:
        """Create a rule: tell me when METRIC of SUBJECT is OP THRESHOLD.

        Example: kterm alerts add market_moves.change_15m KXBTC-26OCT1219-B73250 abs_gt 0.10
        """
        try:
            decimal_text("threshold", threshold)  # refuse a bad number before asking anything
        except (ValueError, TypeError) as exc:
            raise fail(str(exc)) from exc

        async def work(api: ServerClient) -> Any:
            known = {m.metric: m for m in (await api.alert_metrics()).metrics}
            info = known.get(metric)
            if info is None:
                raise fail(f"unknown metric {metric!r}; see: kterm alerts metrics")
            return await api.create_alert_rule(
                metric,
                op,
                threshold,
                ticker=subject if info.subject == "market" else None,
                event_ticker=subject if info.subject == "event" else None,
                cooldown_seconds=cooldown,
                name=name,
            )

        def words(exc: ApiProblem) -> str:
            return f"{subject} was not found" if isinstance(exc, NotFound) else explain(exc)

        rule = run_with_api(work, explain=words)
        if as_json:
            return emit_json(rule)
        typer.echo(f"rule {rule.id}: {rule.metric} of {rule.ticker or rule.event_ticker} "
                   f"{views.condition(rule.op, rule.threshold, rule.unit)}, "
                   f"at most every {rule.cooldown_seconds}s")  # fmt: skip

    @alerts_app.command("set")
    def alerts_set(
        rule_id: int,
        enabled: Annotated[bool | None, typer.Option("--enable/--disable")] = None,
        threshold: Annotated[str | None, typer.Option(help="A new threshold.")] = None,
        cooldown: Annotated[int | None, typer.Option(help="A new cooldown in seconds.")] = None,
        name: Annotated[str | None, typer.Option(help="A new label.")] = None,
    ) -> None:
        """Change a rule (a new threshold, or enabling it, re-arms it)."""
        if enabled is None and threshold is None and cooldown is None and name is None:
            raise fail(
                "nothing to change: give --enable/--disable, --threshold, --cooldown or --name"
            )
        if threshold is not None:
            try:
                decimal_text("threshold", threshold)
            except (ValueError, TypeError) as exc:
                raise fail(str(exc)) from exc

        async def work(api: ServerClient) -> Any:
            return await api.update_alert_rule(
                rule_id, enabled=enabled, threshold=threshold, cooldown_seconds=cooldown, name=name
            )

        def words(exc: ApiProblem) -> str:
            return f"no rule {rule_id}" if isinstance(exc, NotFound) else explain(exc)

        rule = run_with_api(work, explain=words)
        make_console().print(views.rules_table([rule], rule.id))

    @alerts_app.command("rm")
    def alerts_rm(rule_ids: Annotated[list[int], typer.Argument(help="Rule ids.")]) -> None:
        """Delete rules (their past alerts are kept)."""

        async def work(api: ServerClient) -> list[tuple[int, ApiProblem | None]]:
            done: list[tuple[int, ApiProblem | None]] = []
            for rule_id in rule_ids:
                try:
                    await api.delete_alert_rule(rule_id)
                    done.append((rule_id, None))
                except ApiProblem as exc:
                    done.append((rule_id, exc))
            return done

        failed = 0
        for rule_id, exc in run_with_api(work):
            if exc is None:
                typer.echo(f"deleted rule {rule_id}")
            else:
                failed += 1
                typer.echo(f"rule {rule_id}: {explain(exc)}", err=True)
        if failed:
            raise typer.Exit(code=1)

    return alerts_app
