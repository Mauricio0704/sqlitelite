#include "parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Recursively frees a WHERE expression tree
void free_expr(Expr *expr) {
  if (expr == NULL)
    return;
  free_expr(expr->left);
  free_expr(expr->right);
  if (expr->kind == COMPARISON)
    free(expr->strval);
  free(expr);
}

/* Maps a column-name identifier to its ColumnId. Returns 1 on success. */
int resolve_column(Token token, ColumnId *out) {
  if (token.len_lexeme == 2 && strncmp(token.start_lexeme, "id", 2) == 0)
    *out = COLUMN_ID;
  else if (token.len_lexeme == 4 && strncmp(token.start_lexeme, "name", 4) == 0)
    *out = COLUMN_USERNAME;
  else if (token.len_lexeme == 5 &&
           strncmp(token.start_lexeme, "email", 5) == 0)
    *out = COLUMN_EMAIL;
  else
    return 0;
  return 1;
}

/* A value (in an INSERT) is any bare word: an identifier or an integer. The
 * unquoted grammar can't tell "alice" the name from a column name lexically, so
 * the parser accepts either in value position. */
int is_value_token(TokenType type) {
  return type == TOKEN_IDENTIFIER || type == TOKEN_INT_LITERAL;
}

Expr *parse_comparison(Token *tokens, uint32_t *pos) {
  ColumnId where_col;
  if (!resolve_column(tokens[*pos], &where_col))
    return NULL;
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
  expr->col = where_col;
  expr->op_type = op;

  if (tokens[*pos].type == TOKEN_INT_LITERAL) {
    /* Only the integer column may be compared against an int literal. */
    if (where_col != COLUMN_ID) {
      free_expr(expr);
      return NULL;
    }
    expr->intval = (uint32_t)strtol(tokens[*pos].start_lexeme, NULL, 10);
  } else {
    /* String literals only match the string columns. */
    if (where_col == COLUMN_ID) {
      free_expr(expr);
      return NULL;
    }
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
    left = node;
  }

  return left;
}

Expr *parse_expr(Token *tokens, uint32_t *pos) { return parse_or(tokens, pos); }

PrepareStatus parse_statement(Token *tokens, Statement *statement) {
  switch (tokens[0].type) {
  case TOKEN_KW_SELECT: {
    statement->statement_type = STATEMENT_SELECT;
    statement->projection_count = 0;
    statement->has_where = 0;

    /* Bare SELECT projects every column. */
    if (tokens[1].type == TOKEN_EOF)
      return PREPARE_SUCCESS;
    if (tokens[1].type == TOKEN_OP_ALL) {
      if (tokens[2].type == TOKEN_KW_WHERE) {
        uint32_t wpos = 3; /* first token after WHERE */
        Expr *where_expr = parse_expr(tokens, &wpos);
        if (where_expr == NULL || tokens[wpos].type != TOKEN_EOF) {
          free_expr(where_expr);
          return PREPARE_FAILURE;
        }
        statement->has_where = 1;
        statement->where_expr = where_expr;
        return PREPARE_SUCCESS;
      } else if (tokens[2].type == TOKEN_EOF)
        return PREPARE_SUCCESS;
    }

    /* Otherwise parse a comma-separated list of column identifiers. */
    size_t pos = 1;
    while (1) {
      if (tokens[pos].type != TOKEN_IDENTIFIER ||
          statement->projection_count >= MAX_SELECT_COLUMNS)
        return PREPARE_FAILURE;

      ColumnId col;
      if (!resolve_column(tokens[pos], &col))
        return PREPARE_FAILURE;
      statement->projection[statement->projection_count++] = col;
      pos++;

      if (tokens[pos].type == TOKEN_EOF)
        return PREPARE_SUCCESS;
      if (tokens[pos].type == TOKEN_KW_WHERE) {
        uint32_t wpos = pos + 1; /* first token after WHERE */
        Expr *where_expr = parse_expr(tokens, &wpos);
        if (where_expr == NULL || tokens[wpos].type != TOKEN_EOF) {
          free_expr(where_expr);
          return PREPARE_FAILURE;
        }
        statement->has_where = 1;
        statement->where_expr = where_expr;
        return PREPARE_SUCCESS;
      }
      if (tokens[pos].type != TOKEN_COMMA)
        return PREPARE_FAILURE;
      pos++; /* consume comma; another column must follow */
    }
  }

  case TOKEN_KW_INSERT: {
    if (tokens[1].type != TOKEN_INT_LITERAL ||
        !is_value_token(tokens[2].type) || !is_value_token(tokens[3].type) ||
        tokens[4].type != TOKEN_EOF) {
      printf("Incorrect arguments for insert\n");
      return PREPARE_FAILURE;
    }

    Record *record = &statement->record_to_insert;
    /* Need room for a trailing '\0', so len must be strictly less than the
     * field capacity. */
    if (tokens[2].len_lexeme >= sizeof(record->username) ||
        tokens[3].len_lexeme >= sizeof(record->email)) {
      printf("Incorrect arguments for insert\n");
      return PREPARE_FAILURE;
    }

    statement->statement_type = STATEMENT_INSERT;
    record->id = (uint32_t)strtol(tokens[1].start_lexeme, NULL, 10);
    memcpy(record->username, tokens[2].start_lexeme, tokens[2].len_lexeme);
    record->username[tokens[2].len_lexeme] = '\0';
    memcpy(record->email, tokens[3].start_lexeme, tokens[3].len_lexeme);
    record->email[tokens[3].len_lexeme] = '\0';
    return PREPARE_SUCCESS;
  }

  case TOKEN_KW_DELETE:
    if (tokens[1].type != TOKEN_INT_LITERAL || tokens[2].type != TOKEN_EOF) {
      printf("Incorrect arguments for delete\n");
      return PREPARE_FAILURE;
    }
    statement->statement_type = STATEMENT_DELETE;
    statement->id_to_delete =
        (uint32_t)strtol(tokens[1].start_lexeme, NULL, 10);
    return PREPARE_SUCCESS;

  default:
    return PREPARE_UNRECOGNIZED_COMMAND;
  }
}

/* Parses user input into an executable SQL-like statement structure. */
PrepareStatus prepare_statement(const char *input_buffer,
                                Statement *statement) {
  Token *tokens = lexer(input_buffer);
  PrepareStatus status = parse_statement(tokens, statement);

  free(tokens);
  return status;
}