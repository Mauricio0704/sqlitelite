#ifndef PARSER_H
#define PARSER_H

#include "common.h"
#include "lexer.h"

#include <stdbool.h>
#include <stddef.h>

#define MAX_SELECT_COLUMNS 8
#define MAX_TABLE_COLUMNS 8

typedef enum { COMPARISON, AND_EXPR, OR_EXPR } ExprKind;

typedef struct Expr {
  char *col_name; // set by parser; resolved to col_idx by the analyzer
  int col_idx;
  TokenType op_type;
  uint32_t intval;
  char *strval;
  ExprKind kind;
  struct Expr *left;  // Just for or/and
  struct Expr *right; // Just for or/and
} Expr;

typedef struct {
  Token *toks;
  uint32_t pos;
  bool had_error;
} Parser;

typedef struct {
  Record record;
} InsertStmt;

typedef struct {
  const char *raw_stmt;
  char *new_table_name;
  Schema schema;
} CreateStmt;

typedef struct {
  int has_where;
  Expr *where_expr;
} DeleteStmt;

typedef struct {
  char *projection_names[MAX_TABLE_COLUMNS];
  int projection_idxs[MAX_TABLE_COLUMNS];
  size_t projection_count;
  int has_where;
  Expr *where_expr;
} SelectStmt;

typedef struct {
  int has_where;
  Expr *where_expr;
  char *new_vals_cols[MAX_TABLE_COLUMNS];
  int new_vals_idxs[MAX_TABLE_COLUMNS];
  size_t new_vals_count;
  Value *new_vals;
} UpdateStmt;

typedef struct {
  StatementType type;
  char *table_name;
  union {
    InsertStmt *insert_stmt;
    CreateStmt *create_stmt;
    DeleteStmt *delete_stmt;
    SelectStmt *select_stmt;
    UpdateStmt *update_stmt;
  };
} Statement;

int is_value_token(TokenType type);
Expr *parse_expr(Parser *parser);
void free_expr(Expr *expr); // recursive teardown
void free_select_stmt(SelectStmt *stmt);
void free_delete_stmt(DeleteStmt *stmt);
void free_insert_stmt(InsertStmt *stmt);
void free_create_stmt(CreateStmt *stmt);
void free_update_stmt(UpdateStmt *stmt);
void parse_statement(Parser *parser, const char *raw,
                              Statement *statement);
PrepareStatus prepare_statement(const char *in, Statement *statement);

#endif /* PARSER_H */
