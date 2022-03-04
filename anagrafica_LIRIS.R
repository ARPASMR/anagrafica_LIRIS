#==========================================================================#
#  estrae le info di anagrafica per LIRIS dal DBmeteo e le importa         # 
#  nella tabella anagraficasensori del DB postgres di LIRIS                #
#                                                                          #
#  2022/03=04 AV+MR                                                        #
#==========================================================================#

library(DBI)
library(RMySQL)
library(RPostgreSQL, warn.conflicts = FALSE)

# funzione per gestire eventuali errori
neverstop<-function(){
  print("EE..ERRORE durante l'esecuzione dello script!! Messaggio d'Errore prodotto:")
  quit(status=1)
}
options(show.error.messages=TRUE,error=neverstop)

# connessione al DB
drv<-dbDriver("MySQL")
conn<-try(dbConnect(drv, user=as.character(Sys.getenv("MYSQL_USR")), password=as.character(Sys.getenv("MYSQL_PWD")), dbname=as.character(Sys.getenv("MYSQL_DBNAME")), host=as.character(Sys.getenv("MYSQL_HOST")),port=as.numeric(Sys.getenv("MYSQL_PORT")) ))
#
if (inherits(conn,"try-error")) {
  print( "ERRORE nell'apertura della connessione al DBmeteo \n")
  dbDisconnect(conn)
  rm(conn)
  dbUnloadDriver(drv)
  quit(status=1)
}

# preparazione query
query= paste('SELECT A_Stazioni.IDstazione AS idstazione, IDrete AS idrete, ProprietaStazione AS proprieta, Provincia AS provincia, Comune AS comune, Attributo AS attributo, Truncate(Y(A_Sensori.CoordUTM),0) AS utm_nord, Truncate(X(A_Sensori.CoordUTM),0) AS utm_est, QuotaSensore AS quota, A_Sensori.IDsensore AS idsensore, NOMEtipologia AS nometipologia, (Ifnull(QSedificio,0) + Ifnull(QSsupporto,0)) AS altezza, AggregazioneTemporale AS frequenza, Fiume AS fiume, Bacino AS bacino, NULL AS the_geom, A_Sensori.DataInizio AS datainizio, A_Sensori.DataFine AS datafine, Storico AS storico, NULL AS codice_im, CASE WHEN A_Sensori.IDsensore NOT IN ( SELECT IDsensore FROM A_ListaNera WHERE DataFine IS NULL ) THEN "N" ELSE "S" END AS listanera, CASE WHEN A_Sensori.IDsensore NOT IN ( SELECT IDsensore FROM A_Sensori2Destinazione WHERE Destinazione = 50 AND DataFine IS NULL ) THEN "N" ELSE "S" END AS formweb, CASE WHEN A_Sensori.IDsensore IN ( SELECT IDsensore FROM A_Sensori_specifiche WHERE RiscVent = "NO" ) THEN "NO" WHEN A_Sensori.IDsensore IN ( SELECT IDsensore FROM A_Sensori_specifiche WHERE RiscVent = "yes" AND DataDisistallazione IS NULL ) AND A_Sensori.IDstazione IN ( SELECT A_Stazioni.IDstazione FROM A_Stazioni, A_Sensori WHERE A_Stazioni.IDstazione=A_Sensori.IDstazione AND NOMEtipologia = "RIS" AND IDsensore IN ( SELECT IDsensore FROM A_ListaNera WHERE DataFine IS NULL ) ) THEN "N" WHEN A_Sensori.IDsensore IN ( SELECT IDsensore FROM A_Sensori_specifiche WHERE RiscVent = "yes" AND DataDisistallazione IS NULL ) AND A_Sensori.IDstazione NOT IN ( SELECT A_Stazioni.IDstazione FROM A_Stazioni, A_Sensori WHERE A_Stazioni.IDstazione=A_Sensori.IDstazione AND NOMEtipologia = "RIS" AND IDsensore IN ( SELECT IDsensore FROM A_ListaNera WHERE DataFine IS NULL ) ) THEN "S" WHEN A_Sensori.IDsensore IN ( SELECT IDsensore FROM A_Sensori_specifiche WHERE RiscVent = "yes" AND DataDisistallazione IS NOT NULL ) THEN "NO" END AS risc FROM A_Stazioni, A_Sensori, A_Sensori2Destinazione WHERE A_Stazioni.IDstazione = A_Sensori.IDstazione AND A_Sensori.IDsensore = A_Sensori2Destinazione.IDsensore AND A_Sensori2Destinazione.Destinazione = 14 AND A_Sensori2Destinazione.DataFine IS NULL;',sep="")
#
#--------------------------------------------------------------------------------------------
# stessa query più leggibile (nota: rinomina i nomi dei campi in output minuscoli perchè postgres non accetta maiuscole)
#
# SELECT A_Stazioni.IDstazione AS idstazione,  
  # IDrete AS idrete,  
  # ProprietaStazione AS proprieta,  
  # Provincia AS provincia,  
  # Comune AS comune,  
  # Attributo AS attributo,  
  # Truncate(Y(A_Sensori.CoordUTM),0) AS utm_nord,  
  # Truncate(X(A_Sensori.CoordUTM),0) AS utm_est,  
  # QuotaSensore AS quota,  
  # A_Sensori.IDsensore AS idsensore,  
  # NOMEtipologia AS nometipologia,  
  # (Ifnull(QSedificio,0) + Ifnull(QSsupporto,0)) AS altezza,
  # AggregazioneTemporale AS frequenza,
  # Fiume AS fiume, 
  # Bacino AS bacino, 
  # NULL  AS the_geom, 
  # A_Sensori.DataInizio AS datainizio,
  # A_Sensori.DataFine AS datafine,
  # Storico AS storico,
  # NULL AS codice_im,

# /* Aggiunta del campo "listanera": S/N */
  # CASE
    # WHEN A_Sensori.IDsensore NOT IN
       # (
       # SELECT IDsensore
       # FROM A_ListaNera
       # WHERE DataFine IS NULL
       # )
    # THEN "N"
    # ELSE "S"
  # END
  # AS listanera,

# /* Aggiunta del campo "formweb": S/N */
  # CASE
    # WHEN A_Sensori.IDsensore NOT IN
     # (
      # SELECT IDsensore
      # FROM A_Sensori2Destinazione
      # WHERE Destinazione = 50 AND DataFine IS NULL
      # )
    # THEN "N"
    # ELSE "S"
  # END
  # AS formweb,

# /* Aggiunta del campo "risc": NO, S/N, NULL */ 
  # CASE
    # WHEN A_Sensori.IDsensore IN 
		# (
		# SELECT IDsensore
		# FROM A_Sensori_specifiche
		# WHERE RiscVent = "NO"
		# )
	# THEN "NO"
	
    # WHEN A_Sensori.IDsensore IN 
		# (
		# SELECT IDsensore
		# FROM A_Sensori_specifiche
		# WHERE RiscVent = "yes" AND DataDisistallazione IS NULL
		# )
        # AND A_Sensori.IDstazione IN 
			# (
			# SELECT A_Stazioni.IDstazione
            # FROM A_Stazioni, A_Sensori
            # WHERE A_Stazioni.IDstazione=A_Sensori.IDstazione
            # AND NOMEtipologia = "RIS"
            # AND IDsensore IN 
				# (
				# SELECT IDsensore
				# FROM A_ListaNera
				# WHERE DataFine IS NULL
				# )
			# )
	# THEN "N"
	
    # WHEN A_Sensori.IDsensore IN 
		# (
		# SELECT IDsensore
		# FROM A_Sensori_specifiche
		# WHERE RiscVent = "yes" AND DataDisistallazione IS NULL
		# )
        # AND A_Sensori.IDstazione NOT IN 
			# (
			# SELECT A_Stazioni.IDstazione
            # FROM A_Stazioni, A_Sensori
            # WHERE A_Stazioni.IDstazione=A_Sensori.IDstazione
            # AND NOMEtipologia = "RIS"
            # AND IDsensore IN 
				# (
				# SELECT IDsensore
				# FROM A_ListaNera
				# WHERE DataFine IS NULL
				# )
			# )
	# THEN "S"

    # WHEN A_Sensori.IDsensore IN 
		# (
		# SELECT IDsensore
		# FROM A_Sensori_specifiche
		# WHERE RiscVent = "yes" AND DataDisistallazione IS NOT NULL
		# )
	# THEN "NO"
	
  # END
  # AS risc
 
# FROM A_Stazioni, A_Sensori, A_Sensori2Destinazione  
# WHERE 
  # A_Stazioni.IDstazione = A_Sensori.IDstazione  
  # AND A_Sensori.IDsensore = A_Sensori2Destinazione.IDsensore  
  # AND A_Sensori2Destinazione.Destinazione = 14  
  # AND A_Sensori2Destinazione.DataFine IS NULL
  

## Il 28/12/20 tolgo:
# AND nometipologia='PP';  
#--------------------------------------------------------------------------------------------

# esecuzione query
anagrafica<-try(dbGetQuery(conn,query), silent=TRUE)

# rimozione caratteri accentati
anagrafica$comune<-iconv(anagrafica$comune,from="ISO-8859-1",to="ASCII//TRANSLIT")
anagrafica$attributo<-iconv(anagrafica$attributo,from="ISO-8859-1",to="ASCII//TRANSLIT")

# disconnessione dal DBmeteo 
RetCode<-try(dbDisconnect(conn),silent=TRUE)
if (inherits(RetCode,"try-error")) {
  quit(status=1)
}
rm(conn)
dbUnloadDriver(drv)


###### DB IRIS
# nome del DB
DBNAME=as.character(Sys.getenv("PSQL_DB"))
if (DBNAME==""){
   DBNAME="iris_base"
}

drv_psql<-dbDriver("PostgreSQL")
conn_psql = try(dbConnect(drv_psql, user="postgres", password=as.character(Sys.getenv("PSQL_PWD")), dbname=DBNAME, host="10.10.0.19"))
if (inherits(conn_psql,"try-error")) {
  print( "ERRORE nell'apertura della connessione al DB IRIS \n")
  dbDisconnect(conn_psql)
  rm(conn_psql)
  dbUnloadDriver(drv_psql)
  quit(status=1)
}


# svuotamento tabella 
trunc<-try(dbGetQuery(conn_psql,"TRUNCATE dati_di_base.anagraficasensori"), silent=TRUE)
if (inherits(trunc,"try-error")) {
  print( "ERRORE nel svuotare l'anagrafica del DB IRIS \n")
  dbDisconnect(conn_psql)
  rm(conn_psql)
  dbUnloadDriver(drv_psql)
  quit(status=1)
}


# inserimento dati
inserimento<-try(dbWriteTable(conn_psql, c("dati_di_base","anagraficasensori"), anagrafica,append=TRUE,row.names=FALSE),silent=TRUE)
if (inherits(inserimento,"try-error")) {
  print( "ERRORE nel riempire l'anagrafica del DB IRIS \n")
  dbDisconnect(conn_psql)
  rm(conn_psql)
  dbUnloadDriver(drv_psql)
  quit(status=1)
}

# popolamento campi geometrici e calcolo assegnazione stazioni-zone omogenee IM
#query_update<-paste("UPDATE dati_di_base.anagraficasensori SET the_geom = ST_SetSRID(ST_MakePoint(utm_est, utm_nord), 32632); UPDATE dati_di_base.anagraficasensori SET codice_im = foo.codice_im FROM (SELECT b.codice_im, a.idsensore FROM dati_di_base.anagraficasensori a, dati_di_base.aree_allerta b WHERE st_intersects(a.the_geom, b.the_geom)) AS foo WHERE anagraficasensori.idsensore = foo.idsensore")
query_update<-paste("UPDATE dati_di_base.anagraficasensori SET the_geom = ST_SetSRID(ST_MakePoint(utm_est, utm_nord), 32632); UPDATE dati_di_base.anagraficasensori SET codice_im = foo.ds_zona FROM (SELECT b.ds_zona, a.idsensore FROM dati_di_base.anagraficasensori a, dati_di_base.zone_omogenee_rischio_im_2021 b WHERE st_intersects(a.the_geom, b.the_geom)) AS foo WHERE anagraficasensori.idsensore = foo.idsensore")

update<-try(dbGetQuery(conn_psql,query_update), silent=TRUE)
if (inherits(update,"try-error")) {
  print( "ERRORE nel popolare i campi geometrici dell'anagrafica del DB IRIS \n")
  dbDisconnect(conn_psql)
  rm(conn_psql)
  dbUnloadDriver(drv_psql)
  quit(status=1)
}

# disconnessione dal DB IRIS
RetCode<-try(dbDisconnect(conn_psql),silent=TRUE)
if (inherits(RetCode,"try-error")) {
  quit(status=1)
}
rm(conn_psql)
dbUnloadDriver(drv_psql)

q()
