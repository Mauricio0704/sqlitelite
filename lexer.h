#ifndef LEXER_H
#define LEXER_H

#include <stddef.h>

 typedef enum {
  TOKEN_KW_SELECT,
  TOKEN_KW_INSERT,
  TOKEN_KW_DELETE,
  TOKEN_KW_WHERE,
  TOKEN_KW_AND,
  TOKEN_KW_OR,
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

typedef struct {
  TokenType type;
  char *start_lexeme;
  size_t len_lexeme;
} Token;

Token classify_word(const char *start, size_t len);
Token *lexer(const char *line);

#endif /* LEXER_H */
