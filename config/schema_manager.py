from __future__ import annotations

from config.database import Database


class SchemaManager:

    def __init__(self, database: Database) -> None:
        self._database = database

    def initialize_uc1_schema(self) -> None:
        """Backward-compatible initializer for existing callers."""
        self.initialize_uc4_schema()

    def initialize_uc2_schema(self) -> None:
        """Backward-compatible initializer for existing callers."""
        self.initialize_uc4_schema()

    def initialize_uc3_schema(self) -> None:
        """Backward-compatible initializer for existing callers."""
        self.initialize_uc4_schema()

    def initialize_uc4_schema(self) -> None:
        self._database.ensure_database_exists()

        with self._database.session() as (connection, cursor):
            for statement in self._schema_statements():
                cursor.execute(statement)
            for seed_statement in self._seed_statements():
                cursor.execute(seed_statement)
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

        session_parameters = """
        CREATE TABLE IF NOT EXISTS SESSION_PARAMETERS (
            parameter_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            session_id BIGINT NOT NULL UNIQUE,
            lower_limit DECIMAL(18, 2) NOT NULL,
            upper_limit DECIMAL(18, 2) NOT NULL,
            min_bet DECIMAL(18, 2) NOT NULL,
            max_bet DECIMAL(18, 2) NOT NULL,
            default_win_probability DECIMAL(5, 4) NOT NULL,
            max_session_minutes INT NOT NULL,
            strict_mode BOOLEAN NOT NULL DEFAULT TRUE,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_session_parameters_session
                FOREIGN KEY (session_id)
                REFERENCES SESSIONS(session_id)
                ON DELETE CASCADE,
            CONSTRAINT chk_session_parameters_limit_order CHECK (upper_limit > lower_limit),
            CONSTRAINT chk_session_parameters_bet_order CHECK (max_bet >= min_bet),
            CONSTRAINT chk_session_parameters_probability_range CHECK (
                default_win_probability >= 0 AND default_win_probability <= 1
            )
        )
        """

        betting_strategies = """
        CREATE TABLE IF NOT EXISTS BETTING_STRATEGIES (
            strategy_id TINYINT AUTO_INCREMENT PRIMARY KEY,
            strategy_code VARCHAR(50) NOT NULL UNIQUE,
            strategy_name VARCHAR(100) NOT NULL,
            strategy_type ENUM('FIXED', 'PERCENTAGE', 'PROGRESSIVE') NOT NULL,
            is_progressive BOOLEAN NOT NULL DEFAULT FALSE,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_betting_strategies_active (is_active)
        )
        """

        bets = """
        CREATE TABLE IF NOT EXISTS BETS (
            bet_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            session_id BIGINT NOT NULL,
            gambler_id BIGINT NOT NULL,
            strategy_id TINYINT NULL,
            game_index INT NOT NULL,
            bet_amount DECIMAL(18, 2) NOT NULL,
            win_probability DECIMAL(5, 4) NOT NULL,
            odds_type ENUM('FIXED') NOT NULL DEFAULT 'FIXED',
            odds_value DECIMAL(10, 4) NOT NULL DEFAULT 1.0000,
            potential_win DECIMAL(18, 2) NOT NULL,
            stake_before DECIMAL(18, 2) NOT NULL,
            stake_after DECIMAL(18, 2) NULL,
            is_settled BOOLEAN NOT NULL DEFAULT FALSE,
            placed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_bets_session
                FOREIGN KEY (session_id)
                REFERENCES SESSIONS(session_id)
                ON DELETE CASCADE,
            CONSTRAINT fk_bets_gambler
                FOREIGN KEY (gambler_id)
                REFERENCES GAMBLERS(gambler_id)
                ON DELETE RESTRICT,
            CONSTRAINT fk_bets_strategy
                FOREIGN KEY (strategy_id)
                REFERENCES BETTING_STRATEGIES(strategy_id)
                ON DELETE SET NULL,
            CONSTRAINT chk_bets_amount_positive CHECK (bet_amount > 0),
            CONSTRAINT chk_bets_probability_range CHECK (win_probability >= 0 AND win_probability <= 1),
            INDEX idx_bets_session_game (session_id, game_index),
            INDEX idx_bets_gambler_placed (gambler_id, placed_at)
        )
        """

        game_records = """
        CREATE TABLE IF NOT EXISTS GAME_RECORDS (
            game_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            session_id BIGINT NOT NULL,
            bet_id BIGINT NOT NULL UNIQUE,
            outcome ENUM('WIN', 'LOSS') NOT NULL,
            payout_amount DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
            loss_amount DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
            net_change DECIMAL(18, 2) NOT NULL,
            stake_before DECIMAL(18, 2) NOT NULL,
            stake_after DECIMAL(18, 2) NOT NULL,
            consecutive_win_streak INT NOT NULL DEFAULT 0,
            consecutive_loss_streak INT NOT NULL DEFAULT 0,
            game_duration_ms INT NOT NULL DEFAULT 0,
            resolved_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_game_records_session
                FOREIGN KEY (session_id)
                REFERENCES SESSIONS(session_id)
                ON DELETE CASCADE,
            CONSTRAINT fk_game_records_bet
                FOREIGN KEY (bet_id)
                REFERENCES BETS(bet_id)
                ON DELETE CASCADE,
            CONSTRAINT chk_game_records_stake_non_negative CHECK (stake_after >= 0),
            INDEX idx_game_records_session_resolved (session_id, resolved_at)
        )
        """

        pause_records = """
        CREATE TABLE IF NOT EXISTS PAUSE_RECORDS (
            pause_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            session_id BIGINT NOT NULL,
            pause_reason VARCHAR(255) NOT NULL,
            paused_at DATETIME NOT NULL,
            resumed_at DATETIME NULL,
            pause_seconds INT NULL,
            CONSTRAINT fk_pause_records_session
                FOREIGN KEY (session_id)
                REFERENCES SESSIONS(session_id)
                ON DELETE CASCADE,
            CONSTRAINT chk_pause_records_pause_seconds CHECK (
                pause_seconds IS NULL OR pause_seconds >= 0
            ),
            INDEX idx_pause_records_session_paused (session_id, paused_at),
            INDEX idx_pause_records_session_resumed (session_id, resumed_at)
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
            CONSTRAINT fk_stake_transactions_bet
                FOREIGN KEY (bet_id)
                REFERENCES BETS(bet_id)
                ON DELETE SET NULL,
            CONSTRAINT fk_stake_transactions_game
                FOREIGN KEY (game_id)
                REFERENCES GAME_RECORDS(game_id)
                ON DELETE SET NULL,
            CONSTRAINT chk_stake_transactions_non_negative_balances
                CHECK (balance_before >= 0 AND balance_after >= 0),
            INDEX idx_stake_transactions_gambler_created (gambler_id, created_at),
            INDEX idx_stake_transactions_session_created (session_id, created_at),
            INDEX idx_stake_transactions_bet_created (bet_id, created_at),
            INDEX idx_stake_transactions_game_created (game_id, created_at),
            INDEX idx_stake_transactions_type_created (transaction_type, created_at)
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
            session_parameters,
            betting_strategies,
            bets,
            game_records,
            pause_records,
            stake_transactions,
            running_totals_snapshots,
        )

    @staticmethod
    def _seed_statements() -> tuple[str, ...]:
        return (
            """
            INSERT INTO BETTING_STRATEGIES (
                strategy_code,
                strategy_name,
                strategy_type,
                is_progressive,
                is_active
            )
            VALUES
                ('MANUAL', 'Manual Bet', 'FIXED', FALSE, TRUE),
                ('FIXED_AMOUNT', 'Fixed Amount', 'FIXED', FALSE, TRUE),
                ('PERCENTAGE', 'Percentage of Stake', 'PERCENTAGE', FALSE, TRUE),
                ('MARTINGALE', 'Martingale', 'PROGRESSIVE', TRUE, TRUE)
            ON DUPLICATE KEY UPDATE
                strategy_name = VALUES(strategy_name),
                strategy_type = VALUES(strategy_type),
                is_progressive = VALUES(is_progressive),
                is_active = VALUES(is_active)
            """,
        )
