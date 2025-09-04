#' Create and Load OMOP Vocabulary Tables in Results Schema
#'
#' @description
#' Drops (if exists) and recreates the vocabulary schema in the results database. Then creates all required OMOP vocabulary tables with appropriate indexes and primary keys, and bulk uploads vocabulary data from local CSV files (assumed to be in the directory `"vocabulary_tables"`).
#'
#' @param config List. Configuration object including database credentials and schema names, usually from `config::get()`.
#'
#' @details
#' - The function prompts for interactive confirmation before dropping the existing schema; **all tables in the schema will be deleted.**
#' - All tables (`concept`, `concept_ancestor`, `domain`, `drug_strength`, etc.) are (re)created from scratch with recommended OMOP fields and indexes.
#' - Vocabulary data is loaded from corresponding local CSVs (e.g. `vocabulary_tables/concept.csv`) using the `ResultModelManager::pyUploadCsv()` utility.
#' - Uses the schema name provided in `config$resultsVocabularyDatabaseSchema`.
#'
#' @return Invisible `NULL`. Called for its side effects (recreates and loads tables).
#'
#' @examples
#' \dontrun{
#' createVocabularySchema()
#' }
#'
#' @seealso [ResultModelManager::pyUploadCsv()]
#' @export
createVocabularySchema <- function(config = config::get()) {
  #  DROP schema
  connectionDetails <- getResultsConnectionDetails(config)
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  res <- askYesNo(glue::glue("This will delete and recreate the schema {config$resultsVocabularyDatabaseSchema}, continue?"))
  if (!isTRUE(res))
    return(invisible())

  cli::cli_alert_info("Creating schema and indexes")
sql <- SqlRender::render(
    "DROP SCHEMA IF EXISTS @schema CASCADE;
CREATE SCHEMA @schema;

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @schema.concept (
			concept_id integer NOT NULL,
			concept_name varchar(255) NOT NULL,
			domain_id varchar(20) NOT NULL,
			vocabulary_id varchar(20) NOT NULL,
			concept_class_id varchar(20) NOT NULL,
			standard_concept varchar(10) NULL,
			concept_code varchar(50) NOT NULL,
			valid_start_date date NOT NULL,
			valid_end_date date NOT NULL,
			invalid_reason varchar(10) NULL );
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @schema.vocabulary (
			vocabulary_id varchar(20) NOT NULL,
			vocabulary_name varchar(255) NOT NULL,
			vocabulary_reference varchar(255) NULL,
			vocabulary_version varchar(255) NULL,
			vocabulary_concept_id integer NOT NULL );
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @schema.domain (
			domain_id varchar(20) NOT NULL,
			domain_name varchar(255) NOT NULL,
			domain_concept_id integer NOT NULL );
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @schema.concept_class (
			concept_class_id varchar(20) NOT NULL,
			concept_class_name varchar(255) NOT NULL,
			concept_class_concept_id integer NOT NULL );
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @schema.concept_relationship (
			concept_id_1 integer NOT NULL,
			concept_id_2 integer NOT NULL,
			relationship_id varchar(20) NOT NULL,
			valid_start_date date NOT NULL,
			valid_end_date date NOT NULL,
			invalid_reason varchar(10) NULL );
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @schema.relationship (
			relationship_id varchar(20) NOT NULL,
			relationship_name varchar(255) NOT NULL,
			is_hierarchical varchar(10) NOT NULL,
			defines_ancestry varchar(10) NOT NULL,
			reverse_relationship_id varchar(20) NOT NULL,
			relationship_concept_id integer NOT NULL );
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @schema.concept_synonym (
			concept_id integer NOT NULL,
			concept_synonym_name varchar(1000) NOT NULL,
			language_concept_id integer NOT NULL );
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @schema.concept_ancestor (
			ancestor_concept_id integer NOT NULL,
			descendant_concept_id integer NOT NULL,
			min_levels_of_separation integer NOT NULL,
			max_levels_of_separation integer NOT NULL );
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @schema.source_to_concept_map (
			source_code varchar(50) NOT NULL,
			source_concept_id integer NOT NULL,
			source_vocabulary_id varchar(20) NOT NULL,
			source_code_description varchar(255) NULL,
			target_concept_id integer NOT NULL,
			target_vocabulary_id varchar(20) NOT NULL,
			valid_start_date date NOT NULL,
			valid_end_date date NOT NULL,
			invalid_reason varchar(10) NULL );
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @schema.drug_strength (
			drug_concept_id integer NOT NULL,
			ingredient_concept_id integer NOT NULL,
			amount_value NUMERIC NULL,
			amount_unit_concept_id integer NULL,
			numerator_value NUMERIC NULL,
			numerator_unit_concept_id integer NULL,
			denominator_value NUMERIC NULL,
			denominator_unit_concept_id integer NULL,
			box_size integer NULL,
			valid_start_date date NOT NULL,
			valid_end_date date NOT NULL,
			invalid_reason varchar(10) NULL );

ALTER TABLE @schema.concept  ADD CONSTRAINT xpk_concept PRIMARY KEY (concept_id);
ALTER TABLE @schema.vocabulary  ADD CONSTRAINT xpk_vocabulary PRIMARY KEY (vocabulary_id);
ALTER TABLE @schema.domain  ADD CONSTRAINT xpk_domain PRIMARY KEY (domain_id);
ALTER TABLE @schema.concept_class  ADD CONSTRAINT xpk_concept_class PRIMARY KEY (concept_class_id);
ALTER TABLE @schema.relationship  ADD CONSTRAINT xpk_relationship PRIMARY KEY (relationship_id);


CREATE INDEX idx_concept_concept_id  ON @schema.concept  (concept_id ASC);
CLUSTER @schema.concept  USING idx_concept_concept_id ;
CREATE INDEX idx_concept_code ON @schema.concept (concept_code ASC);
CREATE INDEX idx_concept_vocabluary_id ON @schema.concept (vocabulary_id ASC);
CREATE INDEX idx_concept_domain_id ON @schema.concept (domain_id ASC);
CREATE INDEX idx_concept_class_id ON @schema.concept (concept_class_id ASC);
CREATE INDEX idx_vocabulary_vocabulary_id  ON @schema.vocabulary  (vocabulary_id ASC);
CLUSTER @schema.vocabulary  USING idx_vocabulary_vocabulary_id ;
CREATE INDEX idx_domain_domain_id  ON @schema.domain  (domain_id ASC);
CLUSTER @schema.domain  USING idx_domain_domain_id ;
CREATE INDEX idx_concept_class_class_id  ON @schema.concept_class  (concept_class_id ASC);
CLUSTER @schema.concept_class  USING idx_concept_class_class_id ;
CREATE INDEX idx_concept_relationship_id_1  ON @schema.concept_relationship  (concept_id_1 ASC);
CLUSTER @schema.concept_relationship  USING idx_concept_relationship_id_1 ;
CREATE INDEX idx_concept_relationship_id_2 ON @schema.concept_relationship (concept_id_2 ASC);
CREATE INDEX idx_concept_relationship_id_3 ON @schema.concept_relationship (relationship_id ASC);
CREATE INDEX idx_relationship_rel_id  ON @schema.relationship  (relationship_id ASC);
CLUSTER @schema.relationship  USING idx_relationship_rel_id ;
CREATE INDEX idx_concept_synonym_id  ON @schema.concept_synonym  (concept_id ASC);
CLUSTER @schema.concept_synonym  USING idx_concept_synonym_id ;
CREATE INDEX idx_concept_ancestor_id_1  ON @schema.concept_ancestor  (ancestor_concept_id ASC);
CLUSTER @schema.concept_ancestor  USING idx_concept_ancestor_id_1 ;
CREATE INDEX idx_concept_ancestor_id_2 ON @schema.concept_ancestor (descendant_concept_id ASC);
CREATE INDEX idx_source_to_concept_map_3  ON @schema.source_to_concept_map  (target_concept_id ASC);
CLUSTER @schema.source_to_concept_map  USING idx_source_to_concept_map_3 ;
CREATE INDEX idx_source_to_concept_map_1 ON @schema.source_to_concept_map (source_vocabulary_id ASC);
CREATE INDEX idx_source_to_concept_map_2 ON @schema.source_to_concept_map (target_vocabulary_id ASC);
CREATE INDEX idx_source_to_concept_map_c ON @schema.source_to_concept_map (source_code ASC);
CREATE INDEX idx_drug_strength_id_1  ON @schema.drug_strength  (drug_concept_id ASC);
CLUSTER @schema.drug_strength  USING idx_drug_strength_id_1 ;
CREATE INDEX idx_drug_strength_id_2 ON @schema.drug_strength (ingredient_concept_id ASC);

    ", schema = config$resultsVocabularyDatabaseSchema)

    DatabaseConnector::executeSql(connection, sql)

  # Copy tables from databricks
  vocabTables <- c("concept",
                   "concept_ancestor",
                   "concept_class",
                   "concept_recommended",
                   "concept_relationship",
                   "concept_synonym",
                   "domain",
                   "drug_strength",
                   "ingredient_level",
                   "relationship",
                   "source_to_concept_map",
                   "source_to_source",
                   "source_to_standard",
                   "vocabulary")


  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = config::get("dbms"),
    user = config::get("user"),
    password = config::get("password"),
    connectionString = config::get("connectionString")
  )

  # cdmConnection <- DatabaseConnector::connect(connectionDetails)
  # on.exit(DatabaseConnector::disconnect(cdmConnection), add = TRUE)
  # dir.create("vocabulary_tables", showWarnings = FALSE)
  # # Extract tables in batches and insert them into the results schema
  # purrr::walk(vocabTables, function(table) {
  #   cli::cli_alert_info("Downloading vocabulary table {table}")
  #
  #   filepath <- file.path("vocabulary_tables", paste0(table, ".csv"))
  #   DatabaseConnector::renderTranslateQueryApplyBatched(
  #     cdmConnection,
  #     "SELECT * FROM @vocabulary_schema.@table",
  #     table = table,
  #     vocabulary_schema = config$vocabularyDatabaseSchema,
  #     fun = function(rows, pos) {
  #       readr::write_csv(rows, filepath, append = pos != 1)
  #       invisible()
  #     })
  # })

   purrr::walk(vocabTables, function(table) {
    filepath <- file.path("vocabulary_tables", paste0(table, ".csv"))
    ResultModelManager::pyUploadCsv(connection, table = table, filepath = filepath, schema = config$resultsVocabularyDatabaseSchema)
    cli::cli_alert_success("Inserting vocabulary table {table} complete")
  })
}

#' Create Results Schema and Core Tables for Global Analysis
#'
#' @description
#' Drops and recreates the results schema (for non-vocabulary data) and initializes core results tables for self-controlled cohort and cohort generation packages.
#'
#' @param config List. Configuration object including credentials and schema name, usually from `config::get()`.
#'
#' @details
#' - Prompts interactively before dropping any tables; the entire schema will be dropped and rebuilt.
#' - After creating the schema, templates provided by `SelfControlledCohort` and `CohortGenerator` are used to create all necessary results tables.
#' - Uses the schema set as `config$resultsDatabaseSchema`.
#'
#' @return Invisible `NULL`. Used for its side effects.
#'
#' @examples
#' \dontrun{
#' createGlobalSchema()
#' }
#'
#' @seealso [SelfControlledCohort::createResultsDataModel()], [CohortGenerator::createResultsDataModel()]
#' @export
createGlobalSchema <- function(config = config::get()) {
  connectionDetails <- getResultsConnectionDetails(config)
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  res <- askYesNo(glue::glue("This will delete and recreate the schema {config$resultsDatabaseSchema}, continue?"))
  if (!isTRUE(res))
    return(invisible())

  DatabaseConnector::renderTranslateExecuteSql(connection,
                                               "DROP SCHEMA IF EXISTS @schema CASCADE;
                                               CREATE SCHEMA @schema;",
                                               schema = config$resultsDatabaseSchema)

  SelfControlledCohort::createResultsDataModel(connectionDetails, config$resultsDatabaseSchema)
  CohortGenerator::createResultsDataModel(connectionDetails, config$resultsDatabaseSchema)
}