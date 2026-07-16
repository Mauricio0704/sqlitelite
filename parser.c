#include "parser.h"
#include "btree.h"
#include "common.h"
#include "lexer.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Recursively frees a WHERE expression tree
void free_expr(Expr *expr) {
  if (expr == NULL)
    return;
  free_expr(expr->left);
  free_expr(expr->right);
  if (expr->kind == COMPARISON) {
    free(expr->strval);
    free(expr->col_name);
  }
  free(expr);
}

void free_select_stmt(SelectStmt *stmt) {
  for (size_t i = 0; i < stmt->projection_count; i++)
    free(stmt->projection_names[i]);
  if (stmt->has_where)
    free_expr(stmt->where_expr);
  free(stmt);
}

void free_delete_stmt(DeleteStmt *stmt) {
  if (stmt->has_where)
    free_expr(stmt->where_expr);
  free(stmt);
}

void free_insert_stmt(InsertStmt *stmt) {
  free_record(&stmt->record);
  free(stmt);
}

void free_create_stmt(CreateStmt *stmt) {
  Schema *schema = &stmt->schema;
  for (size_t i = 0; i < schema->n_cols; i++)
    free(schema->col_names[i]);
  free(schema->col_names);
  free(schema->col_types);
  free(stmt->new_table_name);
  free(stmt);
}

int is_value_token(TokenType type) {
  return type == TOKEN_IDENTIFIER || type == TOKEN_INT_LITERAL;
}

/* Consume the current token if it matches */
Token *consume(Parser *parser, TokenType type) {
  if (parser->had_error)
    return NULL;
  if (parser->toks[parser->pos].type != type) {
    parser->had_error = true;
    return NULL;
  }
  return &parser->toks[parser->pos++];
}

/* True if the current token is 'type' */
uint8_t check(Parser *parser, TokenType type) {
  if (parser->had_error || parser->toks[parser->pos].type == TOKEN_EOF)
    return 0;
  return parser->toks[parser->pos].type == type;
}

/* Consume and report success only if the current token is 'type'. */
uint8_t match(Parser *parser, TokenType type) {
  if (!check(parser, type))
    return 0;
  parser->pos++;
  return 1;
}

Expr *expr_node(ExprKind kind, Expr *left, Expr *right) {
  Expr *expr = malloc(sizeof(Expr));
  expr->kind = kind;
  expr->left = left;
  expr->right = right;
  expr->strval = NULL;
  expr->col_name = NULL;
  return expr;
}

Expr *parse_comparison(Parser *parser) {
  Token *col_tok = consume(parser, TOKEN_IDENTIFIER);
  if (col_tok == NULL)
    return NULL;

  Token *op_tok = &parser->toks[parser->pos];
  if (op_tok->type != TOKEN_OP_EQUAL && op_tok->type != TOKEN_OP_GREATER &&
      op_tok->type != TOKEN_OP_SMALLER && op_tok->type != TOKEN_OP_GEQ &&
      op_tok->type != TOKEN_OP_SEQ) {
    parser->had_error = true;
    return NULL;
  }
  parser->pos++;

  Token *val_tok = &parser->toks[parser->pos];
  if (!is_value_token(val_tok->type)) {
    parser->had_error = true;
    return NULL;
  }
  parser->pos++;

  Expr *expr = expr_node(COMPARISON, NULL, NULL);
  expr->col_name = strndup(col_tok->start_lexeme, col_tok->len_lexeme);
  expr->op_type = op_tok->type;
  if (val_tok->type == TOKEN_INT_LITERAL)
    expr->intval = (uint32_t)strtol(val_tok->start_lexeme, NULL, 10);
  else
    expr->strval = strndup(val_tok->start_lexeme, val_tok->len_lexeme);

  return expr;
}

/* A primary is either a parenthesized expression or a comparison. */
Expr *parse_primary(Parser *parser) {
  if (match(parser, TOKEN_KW_LPAREN)) {
    Expr *expr = parse_expr(parser);
    if (expr == NULL)
      return NULL;
    if (consume(parser, TOKEN_KW_RPAREN) == NULL) {
      free_expr(expr);
      return NULL;
    }
    return expr;
  }
  return parse_comparison(parser);
}

Expr *parse_and(Parser *parser) {
  Expr *left = parse_primary(parser);
  if (left == NULL)
    return NULL;

  while (match(parser, TOKEN_KW_AND)) {
    Expr *right = parse_primary(parser);
    if (right == NULL) {
      free_expr(left);
      return NULL;
    }
    left = expr_node(AND_EXPR, left, right);
  }

  return left;
}

Expr *parse_or(Parser *parser) {
  Expr *left = parse_and(parser);
  if (left == NULL)
    return NULL;

  while (match(parser, TOKEN_KW_OR)) {
    Expr *right = parse_and(parser);
    if (right == NULL) {
      free_expr(left);
      return NULL;
    }
    left = expr_node(OR_EXPR, left, right);
  }

  return left;
}

Expr *parse_expr(Parser *parser) { return parse_or(parser); }

PrepareStatus parse_column(Parser *parser, Schema *schema) {
  if (schema->n_cols >= MAX_TABLE_COLUMNS) {
    parser->had_error = true;
    return PREPARE_FAILURE;
  }

  Token *name = consume(parser, TOKEN_IDENTIFIER);
  if (name == NULL)
    return PREPARE_FAILURE;

  schema->col_names[schema->n_cols] =
      strndup(name->start_lexeme, name->len_lexeme);

  if (match(parser, TOKEN_KW_INT)) {
    schema->col_types[schema->n_cols] = INT;
  } else if (match(parser, TOKEN_KW_TEXT)) {
    schema->col_types[schema->n_cols] = TEXT;
  } else {
    parser->had_error = true;
    return PREPARE_FAILURE;
  }

  if (match(parser, TOKEN_KW_PRIMARY)) {
    if (consume(parser, TOKEN_KW_KEY) == NULL)
      return PREPARE_FAILURE;
    schema->pk_idx = schema->n_cols;
  }

  schema->n_cols++;
  return PREPARE_SUCCESS;
}

PrepareStatus parse_schema(Parser *parser, Schema *schema) {
  consume(parser, TOKEN_KW_LPAREN);

  do {
    parse_column(parser, schema);
  } while (match(parser, TOKEN_COMMA));

  consume(parser, TOKEN_KW_RPAREN);

  return parser->had_error ? PREPARE_FAILURE : PREPARE_SUCCESS;
}

PrepareStatus parse_create(Parser *parser, const char *raw, Statement *stmt) {
  consume(parser, TOKEN_KW_CREATE);
  consume(parser, TOKEN_KW_TABLE);
  Token *table_name = consume(parser, TOKEN_IDENTIFIER);
  if (parser->had_error)
    return PREPARE_FAILURE;

  stmt->type = STATEMENT_CREATE;
  stmt->table_name = strdup("catalog");

  stmt->create_stmt = malloc(sizeof(CreateStmt));
  stmt->create_stmt->raw_stmt = raw;
  stmt->create_stmt->new_table_name =
      strndup(table_name->start_lexeme, table_name->len_lexeme);

  Schema *schema = &stmt->create_stmt->schema;
  schema->col_names = malloc(sizeof(char *) * MAX_TABLE_COLUMNS);
  schema->col_types = malloc(sizeof(ColumnType) * MAX_TABLE_COLUMNS);
  schema->n_cols = 0;
  schema->pk_idx = UINT32_MAX;

  parse_schema(parser, schema);
  consume(parser, TOKEN_EOF);

  if (parser->had_error) {
    free_create_stmt(stmt->create_stmt);
    free(stmt->table_name);
    return PREPARE_FAILURE;
  }

  return PREPARE_SUCCESS;
}

PrepareStatus parse_select(Parser *parser, Statement *stmt) {
  consume(parser, TOKEN_KW_SELECT);

  stmt->type = STATEMENT_SELECT;
  stmt->select_stmt = malloc(sizeof(SelectStmt));
  SelectStmt *sel = stmt->select_stmt;
  sel->projection_count = 0;
  sel->has_where = 0;

  /* SELECT * : projection_count 0 means "all columns". Otherwise parse a
     comma-separated list of column identifiers. */
  if (!match(parser, TOKEN_OP_ALL)) {
    do {
      Token *col = consume(parser, TOKEN_IDENTIFIER);
      if (col == NULL || sel->projection_count >= MAX_SELECT_COLUMNS) {
        parser->had_error = true;
        free_select_stmt(sel);
        return PREPARE_FAILURE;
      }
      sel->projection_names[sel->projection_count++] =
          strndup(col->start_lexeme, col->len_lexeme);
    } while (match(parser, TOKEN_COMMA));
  }

  Token *from = consume(parser, TOKEN_KW_FROM);
  Token *table = consume(parser, TOKEN_IDENTIFIER);
  if (from == NULL || table == NULL) {
    free_select_stmt(sel);
    return PREPARE_FAILURE;
  }
  stmt->table_name = strndup(table->start_lexeme, table->len_lexeme);

  /* No WHERE clause. */
  if (parser->toks[parser->pos].type == TOKEN_EOF)
    return PREPARE_SUCCESS;

  if (!match(parser, TOKEN_KW_WHERE)) {
    free_select_stmt(sel);
    free(stmt->table_name);
    return PREPARE_FAILURE;
  }

  Expr *where_expr = parse_expr(parser);
  if (where_expr == NULL || consume(parser, TOKEN_EOF) == NULL) {
    free_expr(where_expr);
    free_select_stmt(sel);
    free(stmt->table_name);
    return PREPARE_FAILURE;
  }
  sel->has_where = 1;
  sel->where_expr = where_expr;
  return PREPARE_SUCCESS;
}

PrepareStatus parse_insert(Parser *parser, Statement *stmt) {
  consume(parser, TOKEN_KW_INSERT);
  consume(parser, TOKEN_KW_INTO);
  Token *table = consume(parser, TOKEN_IDENTIFIER);
  consume(parser, TOKEN_KW_VALUES);
  consume(parser, TOKEN_KW_LPAREN);
  if (parser->had_error)
    return PREPARE_FAILURE;

  stmt->type = STATEMENT_INSERT;
  stmt->table_name = strndup(table->start_lexeme, table->len_lexeme);

  stmt->insert_stmt = malloc(sizeof(InsertStmt));
  Record *record = &stmt->insert_stmt->record;
  record->n_vals = 0;
  record->vals = malloc(sizeof(Value) * MAX_TABLE_COLUMNS);

  if (!check(parser, TOKEN_KW_RPAREN)) {
    do {
      if (record->n_vals >= MAX_TABLE_COLUMNS) {
        parser->had_error = true;
        break;
      }
      Token *v = &parser->toks[parser->pos];
      if (v->type == TOKEN_INT_LITERAL) {
        record->vals[record->n_vals].type = INT;
        record->vals[record->n_vals].int_val =
            (uint32_t)strtol(v->start_lexeme, NULL, 10);
        record->n_vals++;
        parser->pos++;
      } else if (v->type == TOKEN_IDENTIFIER) {
        Value *curr_value = &record->vals[record->n_vals];
        curr_value->type = TEXT;
        curr_value->text_val.len = v->len_lexeme;
        curr_value->text_val.str = strndup(v->start_lexeme, v->len_lexeme);
        record->n_vals++;
        parser->pos++;
      } else {
        parser->had_error = true;
        break;
      }
    } while (match(parser, TOKEN_COMMA));
  }

  if (consume(parser, TOKEN_KW_RPAREN) == NULL ||
      consume(parser, TOKEN_EOF) == NULL) {
    free_insert_stmt(stmt->insert_stmt);
    free(stmt->table_name);
    return PREPARE_FAILURE;
  }
  return PREPARE_SUCCESS;
}

PrepareStatus parse_delete(Parser *parser, Statement *stmt) {
  consume(parser, TOKEN_KW_DELETE);
  consume(parser, TOKEN_KW_FROM);
  Token *table_name = consume(parser, TOKEN_IDENTIFIER);
  if (parser->had_error) {
    printf("Incorrect arguments for delete\n");
    return PREPARE_FAILURE;
  }

  stmt->type = STATEMENT_DELETE;
  stmt->delete_stmt = malloc(sizeof(DeleteStmt));
  stmt->table_name = strndup(table_name->start_lexeme, table_name->len_lexeme);

  /* No WHERE clause: delete every row. */
  if (parser->toks[parser->pos].type == TOKEN_EOF) {
    stmt->delete_stmt->has_where = 0;
    stmt->delete_stmt->where_expr = NULL;
    return PREPARE_SUCCESS;
  }

  if (!match(parser, TOKEN_KW_WHERE)) {
    printf("Incorrect arguments for delete\n");
    free(stmt->table_name);
    free(stmt->delete_stmt);
    return PREPARE_FAILURE;
  }

  Expr *where_expr = parse_expr(parser);
  if (where_expr == NULL || consume(parser, TOKEN_EOF) == NULL) {
    free_expr(where_expr);
    free(stmt->table_name);
    free(stmt->delete_stmt);
    return PREPARE_FAILURE;
  }
  stmt->delete_stmt->has_where = 1;
  stmt->delete_stmt->where_expr = where_expr;

  return PREPARE_SUCCESS;
}

PrepareStatus parse_statement(Parser *parser, const char *raw,
                              Statement *stmt) {
  switch (parser->toks[parser->pos].type) {
  case TOKEN_KW_CREATE:
    return parse_create(parser, raw, stmt);
  case TOKEN_KW_SELECT:
    return parse_select(parser, stmt);
  case TOKEN_KW_INSERT:
    return parse_insert(parser, stmt);
  case TOKEN_KW_DELETE:
    return parse_delete(parser, stmt);
  default:
    return PREPARE_UNRECOGNIZED_COMMAND;
  }
}

void debug_lexer(Token *tokens) {
  int i = 0;
  while (tokens[i++].type != TOKEN_EOF) {
    printf("[%s] ", token_type_names[tokens[i].type]);
  }
  printf("\n");
}

/* Parses user input into an executable SQL-like statement structure. */
PrepareStatus prepare_statement(const char *input_buffer, Statement *stmt) {
  Token *toks = lexer(input_buffer);
  Parser parser = {.toks = toks, .pos = 0, .had_error = false};
  PrepareStatus status = parse_statement(&parser, input_buffer, stmt);

  free(toks);
  return status;
}
