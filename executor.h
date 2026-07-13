#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "btree.h"
#include "parser.h"

#include <stdint.h>

ExecuteStatus execute_insert(InsertStmt *statement, Table *table);
void execute_select(SelectStmt *statement, Table *table);
void execute_delete(DeleteStmt *statement, Table *table);
void execute_create(CreateStmt *statement, Table *table);
ExecuteStatus execute_statement(Statement *statement, Database *database);
uint8_t apply_operator(TokenType op_type, int cmp);
uint8_t row_matches(Expr *where_expr, Record record);  // recursive eval
void print_row(Record record, const ColumnId *projection,
                       size_t projection_count);

#endif /* EXECUTOR_H */
