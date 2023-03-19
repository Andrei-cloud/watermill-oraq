package aq

import (
	"context"
	"database/sql"
)

type Transactor interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
	Close() error
}

// // VVVVVV OLD CODE VVVVVV
// type aqConfig struct {
// 	Name           string
// 	Consumer       string
// 	Payload        string
// 	Transformation string
// 	Wait           time.Duration
// 	BatchSize      int
// }

// // Connection -.
// type Conn struct {
// 	db              *sql.DB
// 	params          *godror.ConnectionParams
// 	connectTimeOut  time.Duration
// 	connectTimeWait time.Duration
// 	qConfig         aqConfig
// }

// func New(tns, user, pass string) (*Conn, error) {
// 	var err error
// 	params := &godror.ConnectionParams{}
// 	params.Username, params.Password = user, godror.NewPassword(pass)
// 	params.ConnectString = tns
// 	params.LibDir = os.Getenv("LIB_PATH")
// 	params.StandaloneConnection = true
// 	params.NoTZCheck = true

// 	c := &Conn{
// 		params: params,
// 	}

// 	connectTimeOut := os.Getenv("CONNECT_TIMEOUT")
// 	c.connectTimeOut = 10 * time.Second
// 	if connectTimeOut != "" {
// 		c.connectTimeOut, err = time.ParseDuration(connectTimeOut)
// 		if err != nil {
// 			return nil, err
// 		}
// 	}

// 	connectTimeWait := os.Getenv("CONNECT_TIMEWAIT")
// 	c.connectTimeWait = 1 * time.Second
// 	if connectTimeWait != "" {
// 		c.connectTimeWait, err = time.ParseDuration(connectTimeWait)
// 		if err != nil {
// 			return nil, err
// 		}
// 	}

// 	if err := c.AttemptConnect(context.Background()); err != nil {
// 		return nil, err
// 	}

// 	c.qConfig = aqConfig{
// 		Name:           os.Getenv("QUEUE_NAME"),
// 		Consumer:       os.Getenv("QUEUE_CONSUMER"),
// 		Payload:        os.Getenv("QUEUE_PAYLOAD"),
// 		Transformation: os.Getenv("QUEUE_TRANSFORMATION"),
// 	}
// 	queueWait := os.Getenv("QUEUE_WAIT")
// 	c.qConfig.Wait = 900 * time.Second
// 	if queueWait != "" {
// 		c.qConfig.Wait, err = time.ParseDuration(queueWait)
// 		if err != nil {
// 			return nil, err
// 		}
// 	}

// 	bs := os.Getenv("BATCH_SIZE")
// 	c.qConfig.BatchSize = 1
// 	if bs != "" {
// 		c.qConfig.BatchSize, err = strconv.Atoi(bs)
// 		if err != nil {
// 			slog.Error("parsing batch size", err)
// 			c.qConfig.BatchSize = 1
// 			slog.Warn("using default batch size", slog.Int("batch_size", c.qConfig.BatchSize))
// 		}
// 	}

// 	return c, nil
// }

// func (c *Conn) AttemptConnect(ctx context.Context) error {
// 	ctx, cancel := context.WithTimeout(ctx, c.connectTimeOut)
// 	defer cancel()

// 	for {
// 		slog.Info("attempting to connect", slog.String("tns", c.params.ConnectString))
// 		if ctx.Err() != nil {
// 			return ctx.Err()
// 		}

// 		select {
// 		case <-ctx.Done():
// 			return ctx.Err()
// 		default:
// 			if user, err := c.connect(ctx); err == nil {
// 				slog.Info("connected", slog.String("tns", c.params.ConnectString), slog.String("user", user))
// 				return nil
// 			} else {
// 				if oraErr, ok := godror.AsOraErr(err); ok {
// 					slog.Warn("unable to connect", slog.String("tns", c.params.ConnectString), slog.String("oraCode", oraErr.Error()))
// 				} else {
// 					slog.Warn("unable to connect", slog.String("tns", c.params.ConnectString), slog.String("error", err.Error()))
// 				}
// 				time.Sleep(c.connectTimeWait)
// 			}
// 		}
// 	}
// }

// func (c *Conn) connect(ctx context.Context) (string, error) {
// 	c.db = sql.OpenDB(godror.NewConnector(*c.params))
// 	if err := c.db.PingContext(ctx); err != nil {
// 		return "", err
// 	}

// 	var user string
// 	if err := c.db.QueryRowContext(ctx, "SELECT USER FROM DUAL").Scan(&user); err != nil {
// 		return "", err
// 	}
// 	return user, nil
// }

// func (c *Conn) Db() *sql.DB {
// 	return c.db
// }
// func (c *Conn) Close() {
// 	c.db.Close()
// }

// func (c *Conn) Dequeue(ctx context.Context, f func(msg *[]byte) error) (int, error) {
// 	if err := c.Db().PingContext(ctx); err != nil {
// 		err := c.AttemptConnect(ctx)
// 		return 0, err
// 	}
// 	tx, err := c.db.BeginTx(ctx, nil)
// 	if err != nil {
// 		return 0, err
// 	}
// 	defer tx.Rollback()

// 	q, err := godror.NewQueue(ctx, tx, c.qConfig.Name, c.qConfig.Payload,
// 		godror.WithDeqOptions(godror.DeqOptions{
// 			Consumer:   c.qConfig.Consumer,
// 			Mode:       godror.DeqRemove,
// 			Visibility: godror.VisibleOnCommit,
// 			Navigation: godror.NavNext,
// 			Wait:       c.qConfig.Wait,
// 		}))
// 	if err != nil {
// 		return 0, err
// 	}
// 	defer q.Close()

// 	msgs := make([]godror.Message, c.qConfig.BatchSize) //parametrise this

// 	// Dequeue from raw queue
// 	n, err := q.Dequeue(msgs)
// 	if err != nil {
// 		if oraErr, ok := godror.AsOraErr(err); ok {
// 			if oraErr.Code() == 24010 {
// 				slog.Error("while dequeue", err)
// 				panic("queue does not exist")
// 			}
// 		}
// 		return 0, err
// 	}
// 	if n > 0 {
// 		for _, m := range msgs {
// 			if len(m.Raw) == 0 {
// 				continue
// 			}
// 			if err := f(&m.Raw); err != nil {
// 				slog.Warn("processing record, check DLQ", slog.String("err", err.Error()))
// 			}
// 		}
// 		tx.Commit()
// 		return n, nil
// 	}

// 	return n, nil
// }
