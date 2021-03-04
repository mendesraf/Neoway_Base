package main

import (
	"log"
	"net/http"

	"github.com/jackc/pgx/v4/pgxpool"
)

//realiza a higienização da tabela BASE, que foi populada pelo processamento do arquivo
func higienizarBase(db *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	higienizarBaseCPF(db, w, r);
	higienizarBaseCNPJ(db, w, r);
}

//higieniza do campo CPF
func higienizarBaseCPF(db *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	log.Println("higienizarBaseCPF() - inicio")
	
	//consulta que retornará os CPFs passíveis de higienização
	sql := "SELECT DISTINCT cpf FROM base WHERE cpf IS NOT NULL ORDER BY 1"
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
	
	//realiza a higienização em si para cada CPF retornado na consulta
	w.Write([]byte("\n- Higienizar Base (CPF)"))
	for rows.Next() {
		var vCPF string
		err = rows.Scan(&vCPF)
		if err != nil {
			log.Printf("\nrows interation - error: %v\n", err)
			return
		}
		
		var vUpdateFormato bool

		vCPFFormatado := ""
		vUpdateFormato = false

		//se o CPF possui tamanho 11 e for composto apenas por dígitos, o CPF está sem formatação
		if (len(vCPF) == 11 && allDigit(vCPF)) {
			//insere a formatação no CPF
			vCPFFormatado = formatCPF(vCPF)
			vUpdateFormato = true
		} else {
			vCPFFormatado = vCPF
		}

		vUpdate := ""

		//se o CPF não for válido, será realizada a higienização corrigindo os dígitos verificadores
		if !IsValidCPF(vCPFFormatado) {
			vCPFSemFormatacao := vCPFFormatado
			
			//retira a formatação para corrigir os dígitos
			cleanNonDigits(&vCPFSemFormatacao)

			//calcula o primeiro dígito verificador e realiza a correção no CPF
			d := vCPFSemFormatacao[:9]
			digit := calculateDigit(d, 10)
			vCPFFormatado = vCPFFormatado[0:12] + digit
			
			//calcula o segungo dígito verificador e realiza a correção no CPF
			d = d + digit
			digit = calculateDigit(d, 11)
			vCPFFormatado = vCPFFormatado[0:13] + digit
			
			//monta o update para higienização
			vUpdate = "UPDATE base SET cpf = '"+ vCPFFormatado +"' WHERE cpf = '"+ vCPF +"'"
		} else if vUpdateFormato {
			//se o CPF é válido, a higienização é apenas colocar a formatação
			vUpdate = "UPDATE base SET cpf = '"+ vCPFFormatado +"' WHERE cpf = '"+ vCPF +"'"
		}

		if vUpdate != "" {
			w.Write([]byte("\n  "+ vUpdate))

			//executa o update da higienização
			_, err := db.Exec(r.Context(),vUpdate)
			if err != nil {
				w.Write([]byte("\nNão foi possível atualizar: "+vUpdate))
				log.Printf("db error on insert: %v\n", err)
				continue
			}
		}

		vCPF = vCPFFormatado
	}

	log.Println("higienizarBaseCPF() - fim")
}

//higieniza os campos loja_mais_frequente e loja_ultima_compra, cujo conteúdo é um CNPJ
func higienizarBaseCNPJ(db *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	log.Println("higienizarBaseCNPJ() - inicio")

	// *** Higienização campo loja_mais_frequente - inicio
	//consulta que retornará os CNPJs passíveis de higienização
	sql := "SELECT DISTINCT loja_mais_frequente FROM base WHERE loja_mais_frequente IS NOT NULL ORDER BY 1"
	log.Println("Executar sql: %d", sql)
	
	//realiza a consulta
	rows, err := db.Query(r.Context(), sql)
	log.Println("SQL Executado")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("\nfailed to execute sql in the database"))
		log.Printf("[or] db error: %v\n", err)
		return
	}
	defer rows.Close()
	
	//realiza a higienização em si para cada CNPJ retornado na consulta
	w.Write([]byte("\n- Higienizar Base (CNPJ - Loja Mais Frequente)"))
	for rows.Next() {
		var vCNPJ string
		err = rows.Scan(&vCNPJ)
		if err != nil {
			log.Printf("\nrows interation - error: %v\n", err)
			return
		}
		
		vUpdate := ""
		vCNPJValido := vCNPJ
		
		//se o CNPJ não for válido, será realizada a higienização corrigindo os dígitos verificadores
		if !IsValidCNPJ(vCNPJ) {
			vCNPJSemFormatacao := vCNPJ
			
			//retira a formatação para corrigir os dígitos
			cleanNonDigits(&vCNPJSemFormatacao)

			//calcula o primeiro dígito verificador e realiza a correção no CNPJ
			d := vCNPJSemFormatacao[:12]
			digit := calculateDigit(d, 5)
			vCNPJValido = vCNPJValido[0:16] + digit
			
			//calcula o segundo dígito verificador e realiza a correção no CNPJ
			d = d + digit
			digit = calculateDigit(d, 6)
			vCNPJValido = vCNPJValido[0:17] + digit

			//monta o update para higienização
			vUpdate = "UPDATE base SET loja_mais_frequente = '"+ vCNPJValido +"' WHERE loja_mais_frequente = '"+ vCNPJ +"'"
		}

		if vUpdate != "" {
			w.Write([]byte("\n  "+ vUpdate))

			//executa o update da higienização
			_, err := db.Exec(r.Context(),vUpdate)
			if err != nil {
				w.Write([]byte("Não foi possível atualizar: "+vUpdate))
				log.Printf("db error on insert: %v\n", err)
				continue
			}
		}
	}
	// *** Higienização campo loja_mais_frequente - fim

	// *** Higienização campo loja_ultima_compra - inicio
	//consulta que retornará os CNPJs passíveis de higienização
	sql = "SELECT DISTINCT loja_ultima_compra FROM base WHERE loja_ultima_compra IS NOT NULL ORDER BY 1"
	log.Println("Executar sql: %d", sql)
	
	//realiza a consulta
	rows, err = db.Query(r.Context(), sql)
	log.Println("SQL Executado")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("failed to execute sql in the database"))
		log.Printf("[or] db error: %v\n", err)
		return
	}
	defer rows.Close()
	
	//realiza a higienização em si para cada CNPJ retornado na consulta
	w.Write([]byte("\n- Higienizar Base (CNPJ - Loja Última Compra)"))
	for rows.Next() {
		var vCNPJ string
		err = rows.Scan(&vCNPJ)
		if err != nil {
			log.Printf("rows interation - error: %v\n", err)
			return
		}
		
		vUpdate := ""
		vCNPJValido := vCNPJ
		
		if !IsValidCNPJ(vCNPJ) {
			vCNPJSemFormatacao := vCNPJ
			
			//retira a formatação para corrigir os dígitos
			cleanNonDigits(&vCNPJSemFormatacao)

			//calcula o primeiro dígito verificador e realiza a correção no CNPJ
			d := vCNPJSemFormatacao[:12]
			digit := calculateDigit(d, 5)
			vCNPJValido = vCNPJValido[0:16] + digit
			
			//calcula o segundo dígito verificador e realiza a correção no CNPJ
			d = d + digit
			digit = calculateDigit(d, 6)
			vCNPJValido = vCNPJValido[0:17] + digit

			//monta o update para higienização
			vUpdate = "UPDATE base SET loja_ultima_compra = '"+ vCNPJValido +"' WHERE loja_ultima_compra = '"+ vCNPJ +"'"
		}

		if vUpdate != "" {
			w.Write([]byte("\n  "+ vUpdate))

			//executa o update da higienização
			_, err := db.Exec(r.Context(),vUpdate)
			if err != nil {
				w.Write([]byte("Não foi possível atualizar: "+vUpdate))
				log.Printf("db error on insert: %v\n", err)
				continue
			}
		}
	}
	// *** Higienização campo loja_ultima_compra - fim

	log.Println("higienizarBaseCNPJ() - fim")
}