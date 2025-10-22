using System.Linq;
using System.Windows;
using Microsoft.Msagl.Drawing;
using Microsoft.Msagl.GraphViewerGdi;
using System.Windows.Forms.Integration;

using MsaglDrawing = Microsoft.Msagl.Drawing;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Text;
using System;
using System.Collections.Generic;

namespace frontdruconverteur
{
    public partial class MldWindow : Window
    {
        public MldWindow()
        {
            InitializeComponent();
        }

        // Appelle cette méthode après création de la fenêtre
        public void LoadScript(string sqlScript)
        {
            var tables = ParseSqlToTables(sqlScript);
            DisplayMldGraph(tables);
        }

        // CREATE TABLE <schema?>.<table> ( ... );
        private static readonly Regex CreateTableRegex = new Regex(
            @"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:" +
            @"(?:\[[^\]]+\]|`[^`]+`|""[^""]+""|[A-Za-z_][\w$]*)\s*\.\s*)?" + // schéma optionnel
            @"(?<table>\[[^\]]+\]|`[^`]+`|""[^""]+""|[A-Za-z_][\w$]*)\s*" +   // nom de table
            @"\((?<cols>[\s\S]*?)\)\s*;",                                    // bloc colonnes jusqu'au );
            RegexOptions.IgnoreCase | RegexOptions.Compiled);

        // ALTER TABLE `Owner` ADD CONSTRAINT `Name` FOREIGN KEY (`ColA`, `ColB`) REFERENCES `Ref` (`RColA`, `RColB`);
        // Supporte backticks, quotes, brackets, schéma.optionnel
        private static readonly Regex AlterTableFkRegex = new Regex(
            @"ALTER\s+TABLE\s+(?<table>\[[^\]]+\]|`[^`]+`|""[^""]+""|[A-Za-z_][\w$\.]*)\s+" +
            @"ADD\s+CONSTRAINT\s+(?<cname>\[[^\]]+\]|`[^`]+`|""[^""]+""|[A-Za-z_][\w$]*)\s+" +
            @"FOREIGN\s+KEY\s*\((?<cols>[^)]+)\)\s+" +
            @"REFERENCES\s+(?<reftable>\[[^\]]+\]|`[^`]+`|""[^""]+""|[A-Za-z_][\w$\.]*)" +
            @"(?:\s*\((?<refcols>[^)]+)\))?",
            RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.Multiline);

        public static Dictionary<string, FlattenedTable> ParseSqlToTables(string sql)
        {
            var tables = new Dictionary<string, FlattenedTable>(StringComparer.OrdinalIgnoreCase);

            // 1) CREATE TABLE: colonnes
            foreach (Match match in CreateTableRegex.Matches(sql))
            {
                var tableRaw = match.Groups["table"].Value;
                var tableName = UnquoteIdentifier(tableRaw);
                var colsBlock = match.Groups["cols"].Value;

                var colDefs = SplitTopLevelByComma(colsBlock);

                var columns = new List<Column>();
                foreach (var def in colDefs)
                {
                    var trimmed = TrimSql(def);
                    if (string.IsNullOrWhiteSpace(trimmed))
                        continue;

                    // Ignorer contraintes/indices dans le bloc CREATE TABLE
                    if (StartsWithKeyword(trimmed, "constraint") ||
                        StartsWithKeyword(trimmed, "primary") ||
                        StartsWithKeyword(trimmed, "foreign") ||
                        StartsWithKeyword(trimmed, "unique") ||
                        StartsWithKeyword(trimmed, "key") ||
                        StartsWithKeyword(trimmed, "index") ||
                        StartsWithKeyword(trimmed, ")"))
                    {
                        continue;
                    }

                    var colName = GetFirstIdentifier(trimmed);
                    if (!string.IsNullOrEmpty(colName))
                        columns.Add(new Column { ColumnName = colName });
                }

                tables[tableName] = new FlattenedTable
                {
                    TableName = tableName,
                    Columns = columns,
                    ForeignKeys = new List<ForeignKey>()
                };

                Debug.WriteLine($"[CREATE] Table: {tableName} - {columns.Count} colonnes");
            }

            // 2) ALTER TABLE: foreign keys
            ParseAlterTableForeignKeys(sql, tables);

            return tables;
        }

        private static void ParseAlterTableForeignKeys(string sql, Dictionary<string, FlattenedTable> tables)
        {
            foreach (Match m in AlterTableFkRegex.Matches(sql))
            {
                var tableRaw = m.Groups["table"].Value;
                var colsRaw = m.Groups["cols"].Value;
                var refTableRaw = m.Groups["reftable"].Value;
                // var refColsRaw = m.Groups["refcols"].Success ? m.Groups["refcols"].Value : null; // non utilisé pour l'affichage

                var ownerTable = UnquoteIdentifier(tableRaw);
                var refTable = UnquoteIdentifier(refTableRaw);
                var cols = SplitIdentList(colsRaw);

                if (!tables.TryGetValue(ownerTable, out var owner))
                {
                    owner = new FlattenedTable { TableName = ownerTable };
                    tables[ownerTable] = owner;
                }

                foreach (var col in cols)
                {
                    owner.ForeignKeys.Add(new ForeignKey
                    {
                        ColumnName = col,
                        ReferencedTable = refTable
                    });
                    Debug.WriteLine($"[ALTER FK] {ownerTable}.{col} -> {refTable}");
                }
            }
        }

        private static List<string> SplitIdentList(string list)
        {
            return SplitTopLevelByComma(list)
                .Select(s => UnquoteIdentifier(s.Trim()))
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .ToList();
        }

        private static bool StartsWithKeyword(string s, string keyword)
        {
            return s.Length >= keyword.Length &&
                   s.AsSpan(0, keyword.Length).Equals(keyword, StringComparison.OrdinalIgnoreCase) &&
                   (s.Length == keyword.Length || char.IsWhiteSpace(s[keyword.Length]) || s[keyword.Length] == '(');
        }

        // Retire quotes/backticks/brackets si présents (et gère schema.table)
        private static string UnquoteIdentifier(string id)
        {
            if (string.IsNullOrWhiteSpace(id)) return id;
            id = id.Trim();

            // Si schema.table -> dépouiller chaque côté
            var dot = FindTopLevelDot(id);
            if (dot > 0)
            {
                var left = id.Substring(0, dot);
                var right = id.Substring(dot + 1);
                return $"{UnquoteIdentifier(left)}.{UnquoteIdentifier(right)}";
            }

            if (id.Length >= 2)
            {
                if ((id[0] == '`' && id[^1] == '`') ||
                    (id[0] == '"' && id[^1] == '"') ||
                    (id[0] == '[' && id[^1] == ']'))
                    return id.Substring(1, id.Length - 2);
            }
            return id;
        }

        private static int FindTopLevelDot(string s)
        {
            bool inSingle = false, inDouble = false, inBacktick = false, inBracket = false;
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (!inDouble && !inBacktick && !inBracket && c == '\'') { inSingle = !inSingle; continue; }
                if (!inSingle && !inBacktick && !inBracket && c == '"') { inDouble = !inDouble; continue; }
                if (!inSingle && !inDouble && !inBracket && c == '`') { inBacktick = !inBacktick; continue; }
                if (!inSingle && !inDouble && !inBacktick && c == '[') { inBracket = true; continue; }
                if (inBracket && c == ']') { inBracket = false; continue; }
                if (!inSingle && !inDouble && !inBacktick && !inBracket && c == '.') return i;
            }
            return -1;
        }

        // Récupère le premier identifiant en tenant compte des quotes/backticks/brackets
        private static string GetFirstIdentifier(string s)
        {
            s = s.TrimStart();
            if (string.IsNullOrEmpty(s)) return "";

            if (s[0] == '`')
            {
                var end = s.IndexOf('`', 1);
                return end > 0 ? UnquoteIdentifier(s[..(end + 1)]) : s.Trim().Split(' ')[0];
            }
            if (s[0] == '"')
            {
                var end = s.IndexOf('"', 1);
                return end > 0 ? UnquoteIdentifier(s[..(end + 1)]) : s.Trim().Split(' ')[0];
            }
            if (s[0] == '[')
            {
                var end = s.IndexOf(']', 1);
                return end > 0 ? UnquoteIdentifier(s[..(end + 1)]) : s.Trim().Split(' ')[0];
            }

            // identifiant non quoté: jusqu'au premier espace/parenthèse
            var i = 0;
            while (i < s.Length && !char.IsWhiteSpace(s[i]) && s[i] != '(')
                i++;
            return s[..i];
        }

        // Trim + suppression commentaires ligne et bloc simples
        private static string TrimSql(string s)
        {
            s = RemoveSqlComments(s);
            return s.Trim();
        }

        private static string RemoveSqlComments(string s)
        {
            // supprime commentaires /* ... */ et -- ... fin de ligne
            var sb = new StringBuilder();
            bool inBlock = false, inLine = false;

            for (int i = 0; i < s.Length; i++)
            {
                if (!inBlock && !inLine && i + 1 < s.Length && s[i] == '/' && s[i + 1] == '*')
                {
                    inBlock = true; i++;
                    continue;
                }
                if (inBlock && i + 1 < s.Length && s[i] == '*' && s[i + 1] == '/')
                {
                    inBlock = false; i++;
                    continue;
                }
                if (!inBlock && !inLine && i + 1 < s.Length && s[i] == '-' && s[i + 1] == '-')
                {
                    inLine = true; i++;
                    continue;
                }
                if (inLine && (s[i] == '\r' || s[i] == '\n'))
                {
                    inLine = false;
                }

                if (!inBlock && !inLine)
                    sb.Append(s[i]);
            }
            return sb.ToString();
        }

        // Split sur virgules de niveau 0 (hors parenthèses et quotes/backticks/brackets)
        private static List<string> SplitTopLevelByComma(string s)
        {
            var parts = new List<string>();
            var sb = new StringBuilder();
            int parenDepth = 0;
            bool inSingle = false, inDouble = false, inBacktick = false, inBracket = false;

            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];

                // gestion quotes (SQL: '' pour échapper le ')
                if (!inDouble && !inBacktick && !inBracket && c == '\'')
                {
                    sb.Append(c);
                    inSingle = !inSingle;
                    if (inSingle && i + 1 < s.Length && s[i + 1] == '\'')
                    {
                        sb.Append('\''); i++;
                    }
                    continue;
                }
                if (!inSingle && !inBacktick && !inBracket && c == '"') { sb.Append(c); inDouble = !inDouble; continue; }
                if (!inSingle && !inDouble && !inBracket && c == '`') { sb.Append(c); inBacktick = !inBacktick; continue; }
                if (!inSingle && !inDouble && !inBacktick && c == '[') { sb.Append(c); inBracket = true; continue; }
                if (inBracket && c == ']') { sb.Append(c); inBracket = false; continue; }

                if (!inSingle && !inDouble && !inBacktick && !inBracket)
                {
                    if (c == '(') { parenDepth++; sb.Append(c); continue; }
                    if (c == ')') { parenDepth = Math.Max(0, parenDepth - 1); sb.Append(c); continue; }

                    if (c == ',' && parenDepth == 0)
                    {
                        parts.Add(sb.ToString());
                        sb.Clear();
                        continue;
                    }
                }

                sb.Append(c);
            }

            if (sb.Length > 0)
                parts.Add(sb.ToString());

            return parts;
        }

        private void DisplayMldGraph(Dictionary<string, FlattenedTable> tables)
        {
            var graph = new MsaglDrawing.Graph("MLD");

            // Mémorise les tables connues (issues de CREATE TABLE)
            var knownTables = new HashSet<string>(tables.Keys, StringComparer.OrdinalIgnoreCase);

            // Nœuds
            foreach (var table in tables.Values)
            {
                var node = graph.AddNode(table.TableName);
                node.Attr.Shape = MsaglDrawing.Shape.Box;
                node.Attr.FillColor = MsaglDrawing.Color.LightBlue;
                node.LabelText = table.TableName + "\n" + string.Join("\n", table.Columns.Select(c => c.ColumnName));
            }

            // Arêtes FK + nœuds stub si manquants
            foreach (var table in tables.Values)
            {
                foreach (var fk in table.ForeignKeys)
                {
                    if (string.IsNullOrWhiteSpace(fk.ReferencedTable)) continue;

                    // Crée un stub si la table de référence n'a pas été créée
                    if (!knownTables.Contains(fk.ReferencedTable))
                    {
                        var stub = graph.FindNode(fk.ReferencedTable) ?? graph.AddNode(fk.ReferencedTable);
                        stub.Attr.Shape = MsaglDrawing.Shape.Box;
                        stub.Attr.FillColor = MsaglDrawing.Color.LightGray;
                        if (string.IsNullOrEmpty(stub.LabelText))
                            stub.LabelText = fk.ReferencedTable;
                    }

                    var edge = graph.AddEdge(table.TableName, fk.ReferencedTable);
                    edge.Attr.ArrowheadAtTarget = MsaglDrawing.ArrowStyle.Normal;
                    if (!string.IsNullOrWhiteSpace(fk.ColumnName))
                        edge.LabelText = fk.ColumnName;
                }
            }

            // Affichage dans WPF via WindowsFormsHost
            var host = new WindowsFormsHost();
            var viewer = new GViewer { Graph = graph };
            host.Child = viewer;

            MldContainer.Children.Clear();
            MldContainer.Children.Add(host);
        }
    }

    // Classes simplifiées
    public class FlattenedTable
    {
        public string TableName { get; set; }
        public List<Column> Columns { get; set; } = new();
        public List<ForeignKey> ForeignKeys { get; set; } = new();
    }

    public class Column { public string ColumnName { get; set; } }

    public class ForeignKey
    {
        public string ColumnName { get; set; }
        public string ReferencedTable { get; set; }
    }
}