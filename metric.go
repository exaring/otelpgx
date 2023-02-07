package otelpgx

const (
	pgxPoolAcquireCount            = "pgxpool_acquires"
	pgxpoolAcquireDuration         = "pgxpool_acquire_duration"
	pgxpoolAcquiredConns           = "pgxpool_acquired_conns"
	pgxpoolCancelledAcquires       = "pgxpool_canceled_acquires"
	pgxpoolConstructingConns       = "pgxpool_constructing_conns"
	pgxpoolEmptyAcquire            = "pgxpool_empty_acquire"
	pgxpoolIdleConns               = "pgxpool_idle_conns"
	pgxpoolMaxConns                = "pgxpool_max_conns"
	pgxpoolMaxIdleDestroyCount     = "pgxpool_max_idle_destroys"
	pgxpoolMaxLifetimeDestroyCount = "pgxpool_max_lifetime_destroys"
	pgxpoolNewConnsCount           = "pgxpool_new_conns"
	pgxpoolTotalConns              = "pgxpool_total_conns"
)
