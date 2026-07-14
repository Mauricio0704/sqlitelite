#include "parser.h"
#include "common.h"
#include "lexer.h"
#include <_string.h>
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

int is_value_token(TokenType type) {
  return type == TOKEN_IDENTIFIER || type == TOKEN_INT_LITERAL;
}

Expr *parse_comparison(Token *tokens, uint32_t *pos) {
  if (tokens[*pos].type != TOKEN_IDENTIFIER)
    return NULL;
  Token col_tok = tokens[*pos];
  (*pos)++;
  TokenType op;
  if (tokens[*pos].type != TOKEN_OP_EQUAL &&
      tokens[*pos].type != TOKEN_OP_GREATER &&
      tokens[*pos].type != TOKEN_OP_SMALLER &&
      tokens[*pos].type != TOKEN_OP_GEQ && tokens[*pos].type != TOKEN_OP_SEQ)
    return NULL;
  op = tokens[*pos].type;
  (*pos)++;

  if (!is_value_token(tokens[*pos].type))
    return NULL;

  Expr *expr = malloc(sizeof(Expr));
  expr->kind = COMPARISON;
  expr->left = NULL;
  expr->right = NULL;
  expr->strval = NULL;
  expr->col_name = strndup(col_tok.start_lexeme, col_tok.len_lexeme);
  expr->op_type = op;

  if (tokens[*pos].type == TOKEN_INT_LITERAL) {
    expr->intval = (uint32_t)strtol(tokens[*pos].start_lexeme, NULL, 10);
  } else {
    expr->strval = malloc(sizeof(char) * (tokens[*pos].len_lexeme + 1));
    memcpy(expr->strval, tokens[*pos].start_lexeme, tokens[*pos].len_lexeme);
    expr->strval[tokens[*pos].len_lexeme] = '\0';
  }
  (*pos)++;
  return expr;
}

Expr *parse_and(Token *tokens, uint32_t *pos) {
  Expr *left = parse_comparison(tokens, pos);
  if (left == NULL)
    return NULL;

  while (tokens[*pos].type == TOKEN_KW_AND) {
    (*pos)++; /* consume AND */
    Expr *right = parse_comparison(tokens, pos);
    if (right == NULL) {
      free_expr(left);
      return NULL;
    }
    Expr *node = malloc(sizeof(Expr));
    node->kind = AND_EXPR;
    node->left = left;
    node->right = right;
    node->strval = NULL;
    node->col_name = NULL;
    left = node;
  }

  return left;
}

Expr *parse_or(Token *tokens, uint32_t *pos) {
  Expr *left = parse_and(tokens, pos);

  while (tokens[*pos].type == TOKEN_KW_OR) {
    (*pos)++; /* consume OR */
    Expr *right = parse_and(tokens, pos);
    if (right == NULL) {
      free_expr(left);
      return NULL;
    }
    Expr *node = malloc(sizeof(Expr));
    node->kind = OR_EXPR;
    node->left = left;
    node->right = right;
    node->strval = NULL;
    node->col_name = NULL;
    left = node;
  }

  return left;
}

Expr *parse_expr(Token *tokens, uint32_t *pos) { return parse_or(tokens, pos); }

PrepareStatus parse_create(Token *toks, const char *raw, Statement *stmt) {
  if (toks[1].type != TOKEN_KW_TABLE || toks[2].type != TOKEN_IDENTIFIER ||
      toks[3].type != TOKEN_KW_LPAREN)
    return PREPARE_FAILURE;
  stmt->type = STATEMENT_CREATE;
  stmt->table_name = strdup("catalog");

  stmt->create_stmt = malloc(sizeof(CreateStmt));
  stmt->create_stmt->raw_stmt = raw;
  stmt->create_stmt->new_table_name =
      strndup(toks[2].start_lexeme, toks[2].len_lexeme);
  Schema *schema = &stmt->create_stmt->schema;
  schema->col_names = malloc(sizeof(char *) * 8); /* up to 8 cols */
  schema->col_types = malloc(sizeof(ColumnType) * 8);
  schema->n_cols = 0;
  schema->pk_idx = UINT32_MAX;

  size_t pos = 4;
  while (1) {
    if (toks[pos].type != TOKEN_IDENTIFIER)
      return PREPARE_FAILURE;

    schema->col_names[schema->n_cols] =
        strndup(toks[pos].start_lexeme, toks[pos].len_lexeme);
    pos++;
    switch (toks[pos].type) {
    case TOKEN_KW_INT:
      schema->col_types[schema->n_cols] = INT;
      break;
    case TOKEN_KW_TEXT:
      schema->col_types[schema->n_cols] = TEXT;
      break;
    default:
      return PREPARE_FAILURE;
    }

    pos++;
    if (toks[pos].type == TOKEN_KW_PRIMARY &&
        toks[pos + 1].type == TOKEN_KW_KEY) {
      schema->pk_idx = schema->n_cols;
      pos += 2;
    }
    schema->n_cols += 1;
    if (toks[pos].type == TOKEN_KW_RPAREN)
      break;
    if (toks[pos].type != TOKEN_COMMA)
      return PREPARE_FAILURE;
    pos++;
  }
  if (toks[++pos].type != TOKEN_EOF)
    return PREPARE_FAILURE;

  return PREPARE_SUCCESS;
}

PrepareStatus parse_select(Token *toks, Statement *stmt) {
  stmt->type = STATEMENT_SELECT;
  stmt->select_stmt = malloc(sizeof(SelectStmt));
  stmt->select_stmt->projection_count = 0;
  stmt->select_stmt->has_where = 0;

  size_t pos;
  if (toks[1].type == TOKEN_OP_ALL) {
    /* SELECT * : projection_count 0 means "all columns". */
    pos = 2;
  } else {
    /* Parse a comma-separated list of column identifiers. */
    pos = 1;
    while (1) {
      if (toks[pos].type != TOKEN_IDENTIFIER ||
          stmt->select_stmt->projection_count >= MAX_SELECT_COLUMNS)
        return PREPARE_FAILURE;

      stmt->select_stmt->projection_names[stmt->select_stmt->projection_count++] =
          strndup(toks[pos].start_lexeme, toks[pos].len_lexeme);
      pos++;

      if (toks[pos].type != TOKEN_COMMA)
        break;
      pos++; /* consume comma; another column must follow */
    }
  }

  if (toks[pos].type != TOKEN_KW_FROM || toks[pos + 1].type != TOKEN_IDENTIFIER)
    return PREPARE_FAILURE;
  stmt->table_name =
      strndup(toks[pos + 1].start_lexeme, toks[pos + 1].len_lexeme);
  pos += 2;

  if (toks[pos].type == TOKEN_EOF)
    return PREPARE_SUCCESS;
  if (toks[pos].type == TOKEN_KW_WHERE) {
    uint32_t wpos = pos + 1; /* first token after WHERE */
    Expr *where_expr = parse_expr(toks, &wpos);
    if (where_expr == NULL || toks[wpos].type != TOKEN_EOF) {
      free_expr(where_expr);
      return PREPARE_FAILURE;
    }
    stmt->select_stmt->has_where = 1;
    stmt->select_stmt->where_expr = where_expr;
    return PREPARE_SUCCESS;
  }
  return PREPARE_FAILURE;
}

PrepareStatus parse_insert(Token *toks, Statement *stmt) {
  stmt->type = STATEMENT_INSERT;
  if (toks[1].type != TOKEN_KW_INTO || toks[2].type != TOKEN_IDENTIFIER)
    return PREPARE_FAILURE;

  stmt->table_name = malloc(toks[2].len_lexeme + 1);
  memcpy(stmt->table_name, toks[2].start_lexeme, toks[2].len_lexeme);
  stmt->table_name[toks[2].len_lexeme] = '\0';

  stmt->insert_stmt = malloc(sizeof(InsertStmt));

  Record *record = &stmt->insert_stmt->record;
  record->n_vals = 0;
  record->vals = malloc(sizeof(Value) * 8); /* Placeholder */

  uint16_t token_idx = 3;
  while (toks[token_idx].type != TOKEN_EOF) {
    Token curr_token = toks[token_idx];
    switch (curr_token.type) {
    case TOKEN_INT_LITERAL:
      record->vals[record->n_vals].type = INT;
      record->vals[record->n_vals].int_val =
          (uint32_t)strtol(curr_token.start_lexeme, NULL, 10);
      record->n_vals += 1;
      break;
    case TOKEN_IDENTIFIER: {
      Value *curr_value = &(record->vals[record->n_vals]);
      curr_value->type = TEXT;
      curr_value->text_val.len = curr_token.len_lexeme;
      curr_value->text_val.str = malloc(curr_value->text_val.len + 1);
      memcpy(curr_value->text_val.str, curr_token.start_lexeme,
             curr_token.len_lexeme);
      curr_value->text_val.str[curr_token.len_lexeme] = '\0';
      record->n_vals += 1;
      break;
    }
    default:
      return PREPARE_FAILURE;
    }
    token_idx += 1;
  }

  return PREPARE_SUCCESS;
}

PrepareStatus parse_delete(Token *toks, Statement *stmt) {
  stmt->type = STATEMENT_DELETE;
  if (toks[1].type != TOKEN_KW_FROM || toks[2].type != TOKEN_IDENTIFIER ||
      toks[3].type != TOKEN_INT_LITERAL || toks[4].type != TOKEN_EOF) {
    printf("Incorrect arguments for delete\n");
    return PREPARE_FAILURE;
  }
  stmt->delete_stmt = malloc(sizeof(DeleteStmt));
  stmt->table_name = strndup(toks[2].start_lexeme, toks[2].len_lexeme);
  stmt->delete_stmt->id_to_delete =
      (uint32_t)strtol(toks[3].start_lexeme, NULL, 10);
  return PREPARE_SUCCESS;
}

PrepareStatus parse_statement(Token *toks, const char *raw, Statement *stmt) {
  switch (toks[0].type) {
  case TOKEN_KW_CREATE:
    return parse_create(toks, raw, stmt);
  case TOKEN_KW_SELECT:
    return parse_select(toks, stmt);
  case TOKEN_KW_INSERT:
    return parse_insert(toks, stmt);
  case TOKEN_KW_DELETE:
    return parse_delete(toks, stmt);
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
  PrepareStatus status = parse_statement(toks, input_buffer, stmt);

  free(toks);
  return status;
}