#ifndef PARSER_H
#define PARSER_H

#include "common.h"
#include "lexer.h"

#include <stddef.h>

typedef enum { COLUMN_ID, COLUMN_USERNAME, COLUMN_EMAIL } ColumnId;

#define MAX_SELECT_COLUMNS 8

typedef enum { COMPARISON, AND_EXPR, OR_EXPR } ExprKind;

typedef struct Expr {
  int col_idx;
  TokenType op_type;
  uint32_t intval;
  char *strval;
  ExprKind kind;
  struct Expr *left;  // Just for or/and
  struct Expr *right; // Just for or/and
} Expr;

typedef struct {
  StatementType statement_type;
  Record record_to_insert;
  uint32_t id_to_delete;
  /* Ordered projection list. A count of 0 means "all columns" (bare SELECT). */
  ColumnId projection[MAX_SELECT_COLUMNS];
  size_t projection_count;
  int has_where;
  Expr *where_expr;
  Schema schema;
  char *table_name; /* Table where the operation is executed */
  const char *raw_create_stmt; /* For CREATE TABLE, store the original statement */
  char *create_t_name; /* For CREATE TABLE, store the table name */
} Statement;

int  resolve_column(Token token, int *out);
int  is_value_token(TokenType type);
Expr *parse_comparison(Token *tokens, uint32_t *pos);
Expr *parse_and(Token *tokens, uint32_t *pos);
Expr *parse_or(Token *tokens, uint32_t *pos);
Expr *parse_expr(Token *tokens, uint32_t *pos);
void free_expr(Expr *expr);                     // recursive teardown
PrepareStatus parse_statement(Token *tokens, Statement *statement);
PrepareStatus prepare_statement(const char *in, Statement *statement);

#endif /* PARSER_H */
