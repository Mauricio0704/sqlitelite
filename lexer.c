#include "lexer.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Classifies a word into a token:
Token classify_word(const char *start, size_t len) {
  if (len == 6 && strncmp(start, "create", 6) == 0)
    return (Token){TOKEN_KW_CREATE, (char *)start, len};
  if (len == 5 && strncmp(start, "table", 5) == 0)
    return (Token){TOKEN_KW_TABLE, (char *)start, len};
  if (len == 3 && strncmp(start, "int", 3) == 0)
    return (Token){TOKEN_KW_INT, (char *)start, len};
  if (len == 4 && strncmp(start, "into", 4) == 0)
    return (Token){TOKEN_KW_INTO, (char *)start, len};
  if (len == 4 && strncmp(start, "from", 4) == 0)
    return (Token){TOKEN_KW_FROM, (char *)start, len};
  if (len == 4 && strncmp(start, "text", 4) == 0)
    return (Token){TOKEN_KW_TEXT, (char *)start, len};
  if (len == 7 && strncmp(start, "primary", 7) == 0)
    return (Token){TOKEN_KW_PRIMARY, (char *)start, len};
  if (len == 3 && strncmp(start, "key", 3) == 0)
    return (Token){TOKEN_KW_KEY, (char *)start, len};
  if (len == 6 && strncmp(start, "select", 6) == 0)
    return (Token){TOKEN_KW_SELECT, (char *)start, len};
  if (len == 6 && strncmp(start, "insert", 6) == 0)
    return (Token){TOKEN_KW_INSERT, (char *)start, len};
  if (len == 6 && strncmp(start, "delete", 6) == 0)
    return (Token){TOKEN_KW_DELETE, (char *)start, len};
  if (len == 5 && strncmp(start, "where", 5) == 0)
    return (Token){TOKEN_KW_WHERE, (char *)start, len};
  if (len == 3 && strncmp(start, "and", 3) == 0)
    return (Token){TOKEN_KW_AND, (char *)start, len};
  if (len == 2 && strncmp(start, "or", 2) == 0)
    return (Token){TOKEN_KW_OR, (char *)start, len};
  if (len == 1 && strncmp(start, "*", 1) == 0)
    return (Token){TOKEN_OP_ALL, (char *)start, len};
  if (len == 6 && strncmp(start, "values", 6) == 0)
    return (Token){TOKEN_KW_VALUES, (char *)start, len};

  for (size_t i = 0; i < len; i++) {
    if (!isdigit((unsigned char)start[i]))
      return (Token){TOKEN_IDENTIFIER, (char *)start, len};
  }
  return (Token){TOKEN_INT_LITERAL, (char *)start, len};
}

/* Tokenizes one input line. */
Token *lexer(const char *line) {
  size_t line_len = strlen(line);

  /* Upper bound: at most one token per char, plus the EOF terminator. */
  Token *tokens = malloc(sizeof(Token) * (line_len + 1));
  if (tokens == NULL) {
    printf("Unable to allocate tokens\n");
    exit(EXIT_FAILURE);
  }

  size_t curr_token = 0;
  size_t start = 0;
  for (size_t i = 0; i <= line_len; i++) {
    char c = line[i]; /* line[line_len] is the terminating '\0' */
    int at_end = (i == line_len);
    int is_punct =
        (c == ',' || c == '=' || c == '>' || c == '<' || c == '(' || c == ')');

    /* A word token ends at whitespace, at single-char punctuation, or at the
     * terminating '\0'. Punctuation is then emitted as its own token. */
    if (at_end || isspace((unsigned char)c) || is_punct) {
      if (i > start)
        tokens[curr_token++] = classify_word(line + start, i - start);
      if (is_punct) {
        const char *lexeme = line + i; /* fixed before i advances below */
        TokenType type;
        uint32_t len = 1;
        switch (c) {
        case ',':
          type = TOKEN_COMMA;
          break;
        case '=':
          type = TOKEN_OP_EQUAL;
          break;
        case '(':
          type = TOKEN_KW_LPAREN;
          break;
        case ')':
          type = TOKEN_KW_RPAREN;
          break;
        case '>':
          type = TOKEN_OP_GREATER;
          if (line[i + 1] == '=') {
            type = TOKEN_OP_GEQ;
            len = 2;
            i += 1;
          }
          break;
        case '<':
          type = TOKEN_OP_SMALLER;
          if (line[i + 1] == '=') {
            type = TOKEN_OP_SEQ;
            len = 2;
            i += 1;
          }
          break;
        }
        tokens[curr_token++] = (Token){type, (char *)lexeme, len};
      }
      start = i + 1;
    }
  }

  tokens[curr_token] = (Token){TOKEN_EOF, NULL, 0};
  return tokens;
}