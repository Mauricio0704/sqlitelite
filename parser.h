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
  Record record;
} InsertStmt;

typedef struct {
  const char *raw_stmt;
  char *new_table_name;
  Schema schema;
} CreateStmt;

typedef struct {
  uint32_t id_to_delete;
} DeleteStmt;

typedef struct {
  ColumnId projection[MAX_SELECT_COLUMNS];
  size_t projection_count;
  int has_where;
  Expr *where_expr;
} SelectStmt;

typedef struct {
  StatementType type;
  char *table_name;
  union {
    InsertStmt *insert_stmt;
    CreateStmt *create_stmt;
    DeleteStmt *delete_stmt;
    SelectStmt *select_stmt;
  };
} Statement;

int  resolve_column(Token token, int *out);
int  is_value_token(TokenType type);
Expr *parse_comparison(Token *tokens, uint32_t *pos);
Expr *parse_and(Token *tokens, uint32_t *pos);
Expr *parse_or(Token *tokens, uint32_t *pos);
Expr *parse_expr(Token *tokens, uint32_t *pos);
void free_expr(Expr *expr);                     // recursive teardown
PrepareStatus parse_statement(Token *tokens, const char *raw, Statement *statement);
PrepareStatus prepare_statement(const char *in, Statement *statement);

#endif /* PARSER_H */
