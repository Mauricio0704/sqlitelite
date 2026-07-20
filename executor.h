#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "btree.h"
#include "parser.h"

#include <stdint.h>

ExecuteStatus execute_statement(Statement *statement, Database *database);

#endif /* EXECUTOR_H */
