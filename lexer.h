#ifndef LEXER_H
#define LEXER_H

#include <stddef.h>

 typedef enum {
  TOKEN_KW_CREATE,
  TOKEN_KW_TABLE,
  TOKEN_KW_PRIMARY,
  TOKEN_KW_KEY,
  TOKEN_KW_INT,
  TOKEN_KW_TEXT,
  TOKEN_KW_SELECT,
  TOKEN_KW_INSERT,
  TOKEN_KW_INTO,
  TOKEN_KW_VALUES,
  TOKEN_KW_FROM,
  TOKEN_KW_DELETE,
  TOKEN_KW_WHERE,
  TOKEN_KW_AND,
  TOKEN_KW_OR,
  TOKEN_KW_LPAREN,
  TOKEN_KW_RPAREN,
  TOKEN_OP_ALL,
  TOKEN_OP_EQUAL,
  TOKEN_OP_SMALLER,
  TOKEN_OP_GREATER,
  TOKEN_OP_SEQ,
  TOKEN_OP_GEQ,
  TOKEN_COMMA,
  TOKEN_IDENTIFIER,
  TOKEN_INT_LITERAL,
  TOKEN_EOF
} TokenType;

static const char * const token_type_names[] = {
  "CREATE", "TABLE", "PRIMARY", "KEY", "INT", "TEXT", "SELECT", "INSERT",
  "INTO", "VALUES", "FROM", "DELETE", "WHERE", "AND", "OR", "(", ")", "*", "=",
  "<", ">", "<=", ">=", ",", "IDENTIFIER", "INT_LITERAL", "EOF"
};

typedef struct {
  TokenType type;
  char *start_lexeme;
  size_t len_lexeme;
} Token;

Token classify_word(const char *start, size_t len);
Token *lexer(const char *line);

#endif /* LEXER_H */
