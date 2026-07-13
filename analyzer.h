#ifndef ANALYZER_H
#define ANALYZER_H

#include "common.h"
#include "pager.h"

/* Semantic-analysis layer */
int record_matches_schema(Record record, Schema *schema);
ExecuteStatus analyze(Statement *stmt, Database *db, Table **out);
#endif /* ANALYZER_H */
