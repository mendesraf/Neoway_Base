package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

func main() {
	// Set the flags for the logging package to give us the filename in the logs
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	//obtém a conexão com o BD
	dbPool := getDBConnection(context.Background())
	defer dbPool.Close()

	log.Println("starting server...")

	http.HandleFunc("/favicon.ico", doNothing)
	http.HandleFunc("/", homeHandler(dbPool))
	http.HandleFunc("/processar", processar(dbPool))
	http.HandleFunc("/higienizar", higienizar(dbPool))
	log.Fatal(http.ListenAndServe(":8000", nil))

	log.Println("server started...")
}

func doNothing(w http.ResponseWriter, r *http.Request){}

func homeHandler(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Println("homeHandler() - inicio")
		
		//monta a consulta para retornar a quantidade de registros contidos na tabela BASE
		sql := "SELECT count(*) FROM base"
		log.Println("Executar sql: %d", sql)
		
		//realiza a consulta
		rows, err := db.Query(r.Context(), sql)
		log.Println("SQL Executado")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("failed to execute sql in the database"))
			log.Printf("[or] db error: %v\n", err)
			return
		}
		defer rows.Close()
		
		var qtde int
		w.Write([]byte("\nQuantidade de registros na tabela: "))
		for rows.Next() {
			err = rows.Scan(&qtde)
			if err != nil {
				log.Printf("rows interation - error: %v\n", err)
				return
			}
			w.Write([]byte(strconv.Itoa(qtde) +"\n"))
		}

		w.WriteHeader(http.StatusOK)

		log.Println("homeHandler() - fim")
	}
}

func processar(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		processarArquivo(db, w, r)
	}
}

func higienizar(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Println("higienizarBase() - inicio")
		
		w.Write([]byte("\nHigienização da Base:\n"))
		higienizarBase(db, w, r)
		w.Write([]byte("\n\nHigienização concluída!"))

		w.WriteHeader(http.StatusOK)

		log.Println("higienizarBase() - fim")
	}
}

func getDBConnection(ctx context.Context) *pgxpool.Pool {
	// Retrieve the database host address
	host := os.Getenv("DD_DB_HOST")
	if host == "" {
		host = "127.0.0.1"
	}

	const connectionString = "postgres://goland:goland@%s:5432/goland?sslmode=disable"

	// Try connecting to the database a few times before giving up
	// Retry to connect for a while
	var dbPool *pgxpool.Pool
	var err error
	for i := 1; i < 8; i++ {
		log.Printf("trying to connect to the db server (attempt %d)...\n", i)
		dbPool, err = pgxpool.Connect(ctx, fmt.Sprintf(connectionString, host))
		if err == nil {
			break
		}
		log.Printf("got error: %v\n", err)

		// Sleep a bit before trying again
		time.Sleep(time.Duration(i*i) * time.Second)
	}

	// Stop execution if the database was not initialized
	if dbPool == nil {
		log.Fatalln("could not connect to the database")
	}

	// Get a connection from the pool and check if the database connection is active and working
	db, err := dbPool.Acquire(ctx)
	if err != nil {
		log.Fatalf("failed to get connection on startup: %v\n", err)
	}
	if err := db.Conn().Ping(ctx); err != nil {
		log.Fatalln(err)
	}

	// Add the connection back to the pool
	db.Release()

	return dbPool
}