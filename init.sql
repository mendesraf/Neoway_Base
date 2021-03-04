CREATE TABLE IF NOT EXISTS base (
	id serial CONSTRAINT pk_id_base PRIMARY KEY,
	cpf varchar(14) NOT NULL,
	private_is integer DEFAULT 0,
	incompleto integer DEFAULT 0,
	data_ultima_compra date,
	ticket_medio numeric(8,2),
	ticket_ultima_compra numeric(8,2),
	loja_mais_frequente varchar(18),
	loja_ultima_compra varchar(18)
);

CREATE INDEX IF NOT EXISTS i_base_cpf ON base (cpf);
CREATE INDEX IF NOT EXISTS i_base_duc ON base (data_ultima_compra);
CREATE INDEX IF NOT EXISTS i_base_lmf ON base (loja_mais_frequente);
CREATE INDEX IF NOT EXISTS i_base_luc ON base (loja_ultima_compra);