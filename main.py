from __future__ import annotations

import sys

from rich.console import Console

from config.database import Database
from config.schema_manager import SchemaManager
from config.settings import Settings, SettingsError, load_settings
from ui.interactive_menu import InteractiveMenu

console = Console()


def bootstrap() -> tuple[Database, Settings]:
	settings = load_settings()
	database = Database(settings=settings)
	schema_manager = SchemaManager(database=database)
	schema_manager.initialize_schema()

	console.print(
		"[bold green]Startup complete.[/bold green] "
		"Core tables are ready: "
		"GAMBLERS, BETTING_PREFERENCES, SESSIONS, SESSION_PARAMETERS, PAUSE_RECORDS, "
		"BETTING_STRATEGIES, ODDS_CONFIGURATIONS, BETS, GAME_RECORDS, "
		"STAKE_TRANSACTIONS, RUNNING_TOTALS_SNAPSHOTS, VALIDATION_EVENTS."
	)

	return database, settings


def main() -> None:
	try:
		database, settings = bootstrap()
		if sys.stdin.isatty():
			menu = InteractiveMenu(database=database, settings=settings, console=console)
			menu.run()
		else:
			console.print(
				"[yellow]Non-interactive input detected. Interactive menu skipped.[/yellow]"
			)
	except SettingsError as exc:
		console.print(f"[bold red]Invalid configuration:[/bold red] {exc}")
		raise SystemExit(1) from exc
	except Exception as exc:
		console.print(f"[bold red]Application startup failed:[/bold red] {exc}")
		raise SystemExit(1) from exc


if __name__ == "__main__":
	main()