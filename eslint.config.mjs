import neostandard from 'neostandard'

// English-only source: flags non-ASCII characters in identifiers and
// comments. ASCII is the enforceable proxy for the English-only rule;
// accented or non-Latin text cannot slip into names or comments.
const hasNonAscii = (text) => {
  for (const char of text) {
    const codePoint = char.codePointAt(0)
    if (codePoint !== undefined && codePoint > 0x7e) return true
  }
  return false
}

const englishOnlyPlugin = {
  rules: {
    'english-only': {
      meta: {
        type: 'problem',
        docs: {
          description: 'enforce ASCII-only (English) identifiers and comments'
        },
        messages: {
          identifier: 'Identifier "{{name}}" contains non-ASCII characters; use English (ASCII) names.',
          comment: 'Comment contains non-ASCII characters; write comments in English (ASCII).'
        },
        schema: []
      },
      create (context) {
        return {
          Identifier (node) {
            if (hasNonAscii(node.name)) {
              context.report({ node, messageId: 'identifier', data: { name: node.name } })
            }
          },
          Program () {
            for (const comment of context.sourceCode.getAllComments()) {
              if (hasNonAscii(comment.value)) {
                context.report({ loc: comment.loc, messageId: 'comment' })
              }
            }
          }
        }
      }
    }
  }
}

export default [
  ...neostandard({
    ts: true,
    ignores: ['dist', 'reports', '.stryker-tmp']
  }),
  {
    plugins: {
      harbor: englishOnlyPlugin
    },
    rules: {
      'harbor/english-only': 'error',
      'no-restricted-syntax': [
        'error',
        {
          selector: 'TSEnumDeclaration',
          message: 'enum is banned; use a plain object with "as const" instead.'
        }
      ]
    }
  },
  {
    // The core knows nothing about clients: top-level src modules may only
    // import their siblings, never an adapter or the testing entry point.
    // Adapters depend on the core; the reverse would drag the native client
    // into every install.
    files: ['src/*.ts'],
    rules: {
      'no-restricted-imports': [
        'error',
        {
          patterns: [
            {
              group: ['./*/*', './*/**'],
              message: 'the core must not import adapter or testing modules; they depend on the core, never the reverse.'
            }
          ]
        }
      ]
    }
  }
]
