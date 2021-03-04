package main

import (
	"bufio"
	"log"
	"net/http"
	"os"
	"strings"
	"unicode"
	
	"github.com/jackc/pgx/v4/pgxpool"
	"golang.org/x/text/transform"
    "golang.org/x/text/unicode/norm"
)

//processa o arquivo e executa os inserts
func processarArquivo(db *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	log.Println("processarArquivo() - inicio")
	
	//abre o arquivo
	file, err := os.Open("/tmp/base_teste.txt")

	if err != nil {
		log.Fatalf("failed opening file: %s", err)
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var txtlines []string
	
	//extrai cada linha do arquivo
	for scanner.Scan() {
		txtlines = append(txtlines, scanner.Text())
	}
	
	//fecha o arquivo
	file.Close()

	qtd := 0
	index_CPF := 0
	index_PRIVATE := 0
	index_INCOMPLETO := 0
	index_DATA_ULTIMA_COMPRA := 0
	index_TICKET_MEDIO := 0
	index_TICKET_ULTIMA_COMPRA := 0
	index_LOJA_MAIS_FREQUENTE := 0
	index_LOJA_ULTIMA_COMPRA := 0
	for _, eachline := range txtlines {
		//se a quantidade for 0, é a linha referente aos títulos das colunas
		if qtd == 0 {
			qtd++
			
			//retira os acentos dos títulos
			t := transform.Chain(norm.NFD, transform.RemoveFunc(isMn), norm.NFC)
			eachline, _, _ := transform.String(t, eachline)
			
			//identifica o índice onde cada coluna inicia
			index_CPF = strings.Index(eachline, "CPF")
			index_PRIVATE = strings.Index(eachline, "PRIVATE")
			index_INCOMPLETO = strings.Index(eachline, "INCOMPLETO")
			index_DATA_ULTIMA_COMPRA = strings.Index(eachline, "DATA DA ULTIMA COMPRA")
			index_TICKET_MEDIO = strings.Index(eachline, "TICKET MEDIO")
			index_TICKET_ULTIMA_COMPRA = strings.Index(eachline, "TICKET DA ULTIMA COMPRA")
			index_LOJA_MAIS_FREQUENTE = strings.Index(eachline, "LOJA MAIS FREQUENTE")
			index_LOJA_ULTIMA_COMPRA = strings.Index(eachline, "LOJA DA ULTIMA COMPRA")
			
			_, _, _, _, _, _, _, _, _ = qtd, index_CPF, index_PRIVATE, index_INCOMPLETO, index_DATA_ULTIMA_COMPRA, index_TICKET_MEDIO, index_TICKET_ULTIMA_COMPRA, index_LOJA_MAIS_FREQUENTE, index_LOJA_ULTIMA_COMPRA

			vTruncate := "TRUNCATE TABLE base"
			_, _ = db.Exec(r.Context(),vTruncate)
		} else {
			qtd++

			//realiza o split das colunas e carrega nos respectivos campos
			vCPF := strings.TrimSpace(eachline[index_CPF:(index_PRIVATE-1)])
			vPRIVATE := strings.TrimSpace(eachline[index_PRIVATE:(index_INCOMPLETO-1)])
			vINCOMPLETO := strings.TrimSpace(eachline[index_INCOMPLETO:(index_DATA_ULTIMA_COMPRA-1)])
			vDATA_ULTIMA_COMPRA := strings.TrimSpace(eachline[index_DATA_ULTIMA_COMPRA:(index_TICKET_MEDIO-1)])
			vTICKET_MEDIO := strings.TrimSpace(eachline[index_TICKET_MEDIO:(index_TICKET_ULTIMA_COMPRA-1)])
			vTICKET_ULTIMA_COMPRA := strings.TrimSpace(eachline[index_TICKET_ULTIMA_COMPRA:(index_LOJA_MAIS_FREQUENTE-1)])
			vLOJA_MAIS_FREQUENTE := strings.TrimSpace(eachline[index_LOJA_MAIS_FREQUENTE:(index_LOJA_ULTIMA_COMPRA-1)])
			vLOJA_ULTIMA_COMPRA := strings.TrimSpace(eachline[index_LOJA_ULTIMA_COMPRA:len(eachline)])
			
			//monta o insert conforme o conteúdo dos campos
			if vCPF != "NULL" {
				vInsert := "INSERT INTO base(cpf"
				vValues := " VALUES ('"+ strings.TrimSpace(vCPF) +"'"

				if vPRIVATE != "NULL" {
					vInsert = vInsert +",private_is"
					vValues = vValues +","+ strings.TrimSpace(vPRIVATE)
				}

				if vINCOMPLETO != "NULL" {
					vInsert = vInsert +",incompleto"
					vValues = vValues +","+ strings.TrimSpace(vINCOMPLETO)
				}

				if vDATA_ULTIMA_COMPRA != "NULL" {
					vInsert = vInsert +",data_ultima_compra"
					vValues = vValues +",'"+ strings.TrimSpace(vDATA_ULTIMA_COMPRA) +"'"
				}

				if vTICKET_MEDIO != "NULL" {
					vTICKET_MEDIO = strings.TrimSpace(vTICKET_MEDIO)
					vTICKET_MEDIO = strings.Replace(vTICKET_MEDIO, ".", "", -1)
					vTICKET_MEDIO = strings.Replace(vTICKET_MEDIO, ",", ".", -1)
					vInsert = vInsert +",ticket_medio"
					vValues = vValues +","+ vTICKET_MEDIO
				}

				if vTICKET_ULTIMA_COMPRA != "NULL" {
					vTICKET_ULTIMA_COMPRA = strings.TrimSpace(vTICKET_ULTIMA_COMPRA)
					vTICKET_ULTIMA_COMPRA = strings.Replace(vTICKET_ULTIMA_COMPRA, ".", "", -1)
					vTICKET_ULTIMA_COMPRA = strings.Replace(vTICKET_ULTIMA_COMPRA, ",", ".", -1)
					vInsert = vInsert +",ticket_ultima_compra"
					vValues = vValues +","+ vTICKET_ULTIMA_COMPRA
				}

				if vLOJA_MAIS_FREQUENTE != "NULL" {
					vInsert = vInsert +",loja_mais_frequente"
					vValues = vValues +",'"+ strings.TrimSpace(vLOJA_MAIS_FREQUENTE) +"'"
				}

				if vLOJA_ULTIMA_COMPRA != "NULL" {
					vInsert = vInsert +",loja_ultima_compra"
					vValues = vValues +",'"+ strings.TrimSpace(vLOJA_ULTIMA_COMPRA) +"'"
				}

				vInsert = vInsert +")"
				vValues = vValues +")"
				vSQL := vInsert + vValues

				_, err := db.Exec(r.Context(),vSQL)
				if err != nil {
					w.Write([]byte("Não foi possível inserir: "+vSQL))
					log.Printf("db error on insert: %v\n", err)
					continue
				}
			}
		}
		
	}
	log.Println("processarArquivo() - fim")
}

func isMn(r rune) bool {
	return unicode.Is(unicode.Mn, r) // Mn: nonspacing marks
}