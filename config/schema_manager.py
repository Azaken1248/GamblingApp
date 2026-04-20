from __future__ import annotations

from config.database import Database


class SchemaManager:

    def __init__(self, database: Database) -> None:
        self._database = database

    def initialize_uc1_schema(self) -> None:
        """Backward-compatible initializer for existing callers."""
        self.initialize_uc2_schema()

    def initialize_uc2_schema(self) -> None:
        self._database.ensure_database_exists()

        with self._database.session() as (connection, cursor):
            for statement in self._schema_statements():
                cursor.execute(statement)
            connection.commit()

    @staticmethod
    def _schema_statements() -> tuple[str, ...]:
        gamblers = """
        CREATE TABLE IF NOT EXISTS GAMBLERS (
            gambler_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(100) NOT NULL UNIQUE,
            full_name VARCHAR(150) NOT NULL,
            email VARCHAR(150) NOT NULL UNIQUE,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            initial_stake DECIMAL(18, 2) NOT NULL,
            current_stake DECIMAL(18, 2) NOT NULL,
            win_threshold DECIMAL(18, 2) NOT NULL,
            loss_threshold DECIMAL(18, 2) NOT NULL,
            min_required_stake DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            CONSTRAINT chk_gamblers_initial_stake_positive CHECK (initial_stake > 0),
            CONSTRAINT chk_gamblers_current_stake_non_negative CHECK (current_stake >= 0),
            CONSTRAINT chk_gamblers_threshold_order CHECK (win_threshold > loss_threshold)
        )
        """

        betting_preferences = """
        CREATE TABLE IF NOT EXISTS BETTING_PREFERENCES (
            preference_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            gambler_id BIGINT NOT NULL UNIQUE,
            min_bet DECIMAL(18, 2) NOT NULL,
            max_bet DECIMAL(18, 2) NOT NULL,
            preferred_game_type VARCHAR(100) NOT NULL,
            auto_play_enabled BOOLEAN NOT NULL DEFAULT FALSE,
            auto_play_max_games INT NOT NULL DEFAULT 0,
            session_loss_limit DECIMAL(18, 2),
            session_win_target DECIMAL(18, 2),
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            CONSTRAINT fk_betting_preferences_gambler
                FOREIGN KEY (gambler_id)
                REFERENCES GAMBLERS(gambler_id)
                ON DELETE CASCADE,
            CONSTRAINT chk_betting_preferences_min_bet_positive CHECK (min_bet > 0),
            CONSTRAINT chk_betting_preferences_max_bet_gte_min_bet CHECK (max_bet >= min_bet)
        )
        """

        stake_transactions = """
        CREATE TABLE IF NOT EXISTS STAKE_TRANSACTIONS (
            transaction_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            session_id BIGINT NULL,
            gambler_id BIGINT NOT NULL,
            bet_id BIGINT NULL,
            game_id BIGINT NULL,
            transaction_type ENUM(
                'INITIAL_STAKE',
                'BET_PLACED',
                'BET_WIN',
                'BET_LOSS',
                'DEPOSIT',
                'WITHDRAWAL',
                'ADJUSTMENT',
                'RESET'
            ) NOT NULL,
            amount DECIMAL(18, 2) NOT NULL,
            balance_before DECIMAL(18, 2) NOT NULL,
            balance_after DECIMAL(18, 2) NOT NULL,
            transaction_ref VARCHAR(100) NOT NULL UNIQUE,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_stake_transactions_gambler
                FOREIGN KEY (gambler_id)
                REFERENCES GAMBLERS(gambler_id)
                ON DELETE RESTRICT,
            CONSTRAINT fk_stake_transactions_session
                FOREIGN KEY (session_id)
                REFERENCES SESSIONS(session_id)
                ON DELETE SET NULL,
            CONSTRAINT chk_stake_transactions_non_negative_balances
                CHECK (balance_before >= 0 AND balance_after >= 0),
            INDEX idx_stake_transactions_gambler_created (gambler_id, created_at),
            INDEX idx_stake_transactions_session_created (session_id, created_at),
            INDEX idx_stake_transactions_type_created (transaction_type, created_at)
        )
        """

        sessions = """
        CREATE TABLE IF NOT EXISTS SESSIONS (
            session_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            gambler_id BIGINT NOT NULL,
            status ENUM(
                'INITIALIZED',
                'ACTIVE',
                'PAUSED',
                'ENDED_WIN',
                'ENDED_LOSS',
                'ENDED_MANUAL',
                'ENDED_TIMEOUT'
            ) NOT NULL DEFAULT 'INITIALIZED',
            end_reason ENUM(
                'UPPER_LIMIT_REACHED',
                'LOWER_LIMIT_REACHED',
                'MANUAL_STOP',
                'TIMEOUT',
                'NOT_ENDED'
            ) NULL,
            starting_stake DECIMAL(18, 2) NOT NULL,
            ending_stake DECIMAL(18, 2) NULL,
            peak_stake DECIMAL(18, 2) NOT NULL,
            lowest_stake DECIMAL(18, 2) NOT NULL,
            lower_limit DECIMAL(18, 2) NOT NULL,
            upper_limit DECIMAL(18, 2) NOT NULL,
            max_games INT NOT NULL,
            games_played INT NOT NULL DEFAULT 0,
            total_pause_seconds INT NOT NULL DEFAULT 0,
            started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            ended_at DATETIME NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_sessions_gambler
                FOREIGN KEY (gambler_id)
                REFERENCES GAMBLERS(gambler_id)
                ON DELETE RESTRICT,
            CONSTRAINT chk_sessions_limits_order CHECK (upper_limit > lower_limit),
            CONSTRAINT chk_sessions_games_non_negative CHECK (games_played >= 0),
            INDEX idx_sessions_gambler_status (gambler_id, status),
            INDEX idx_sessions_started_at (started_at)
        )
        """

        running_totals_snapshots = """
        CREATE TABLE IF NOT EXISTS RUNNING_TOTALS_SNAPSHOTS (
            snapshot_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            session_id BIGINT NOT NULL,
            game_id BIGINT NULL,
            total_games INT NOT NULL DEFAULT 0,
            total_wins INT NOT NULL DEFAULT 0,
            total_losses INT NOT NULL DEFAULT 0,
            total_pushes INT NOT NULL DEFAULT 0,
            total_winnings DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
            total_losses_amount DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
            net_profit DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
            win_rate DECIMAL(10, 4) NOT NULL DEFAULT 0.0000,
            profit_factor DECIMAL(18, 4) NOT NULL DEFAULT 0.0000,
            roi DECIMAL(18, 4) NOT NULL DEFAULT 0.0000,
            longest_win_streak INT NOT NULL DEFAULT 0,
            longest_loss_streak INT NOT NULL DEFAULT 0,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_running_totals_session
                FOREIGN KEY (session_id)
                REFERENCES SESSIONS(session_id)
                ON DELETE CASCADE,
            INDEX idx_running_totals_session_created (session_id, created_at)
        )
        """

        return (
            gamblers,
            betting_preferences,
            sessions,
            stake_transactions,
            running_totals_snapshots,
        )
