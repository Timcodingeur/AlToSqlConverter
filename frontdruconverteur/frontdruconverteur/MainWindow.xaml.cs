using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Windows;

namespace frontdruconverteur
{
    public partial class MainWindow : Window
    {
        // On stocke la liste des fichiers AL détectés (chemins complets)
        private List<string> tableFilePaths = new List<string>();

        public MainWindow()
        {
            InitializeComponent();
        }

        #region Drag & Drop

        private void Border_DragEnter(object sender, DragEventArgs e)
        {
            if (e.Data.GetDataPresent(DataFormats.FileDrop))
                e.Effects = DragDropEffects.Copy;
            else
                e.Effects = DragDropEffects.None;
        }

        private void Border_Drop(object sender, DragEventArgs e)
        {
            if (e.Data.GetDataPresent(DataFormats.FileDrop))
            {
                string[] droppedItems = (string[])e.Data.GetData(DataFormats.FileDrop);
                // On suppose que le premier élément déposé est un dossier.
                string folderPath = droppedItems[0];

                if (Directory.Exists(folderPath))
                {
                    FileListBox.Items.Clear();
                    tableFilePaths.Clear();
                    try
                    {
                        // Recherche tous les fichiers dans le dossier et ses sous-dossiers.
                        var files = Directory.GetFiles(folderPath, "*", SearchOption.AllDirectories);
                        foreach (var file in files)
                        {
                            try
                            {
                                string firstCodeLine = GetFirstCodeLine(file);
                                if (!string.IsNullOrEmpty(firstCodeLine) &&
                                    firstCodeLine.TrimStart().StartsWith("table", StringComparison.OrdinalIgnoreCase)&& file.Split('.').Last()=="al")  
                                {
                                    FileListBox.Items.Add(Path.GetFileName(file));
                                    tableFilePaths.Add(file);
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Erreur lors de la lecture de {file} : {ex.Message}");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        MessageBox.Show("Erreur lors de la lecture des fichiers : " + ex.Message);
                    }
                }
                else
                {
                    MessageBox.Show("Le chemin déposé n'est pas un dossier valide.");
                }
            }
        }

        private string GetFirstCodeLine(string filePath)
        {
            try
            {
                foreach (var line in File.ReadLines(filePath))
                {
                    string trimmedLine = line.Trim();
                    if (string.IsNullOrEmpty(trimmedLine))
                        continue;
                    if (trimmedLine.StartsWith("//") || trimmedLine.StartsWith("--") || trimmedLine.StartsWith("/*"))
                        continue;
                    return trimmedLine;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erreur lors de la lecture de {filePath} : {ex.Message}");
            }
            return null;
        }

        #endregion

        #region Conversion AL -> SQL

        private class TableDefinition
        {
            public string TableNumber;
            public string TableName;
            public List<ColumnDefinition> Columns { get; set; } = new List<ColumnDefinition>();
            public List<string> PrimaryKeys { get; set; } = new List<string>();
            public List<ForeignKeyDefinition> ForeignKeys { get; set; } = new List<ForeignKeyDefinition>();
        }

        private class ColumnDefinition
        {
            public string ColumnName;
            public string ALType;
            public string SQLType;
            public string TableRelation;
        }

        private class ForeignKeyDefinition
        {
            public string ColumnName;
            public string ReferencedTable;
        }

        private void ConvertButton_Click(object sender, RoutedEventArgs e)
        {
            if (tableFilePaths.Count == 0)
            {
                MessageBox.Show("Aucun fichier AL de table détecté.");
                return;
            }

            List<TableDefinition> tables = new List<TableDefinition>();
            foreach (var file in tableFilePaths)
            {
                TableDefinition tableDef = ParseALFile(file);
                if (tableDef != null)
                    tables.Add(tableDef);
            }

            if (tables.Count == 0)
            {
                MessageBox.Show("Aucune définition de table valide n'a été trouvée dans les fichiers.");
                return;
            }

            string sqlScript = GenerateSQLScript(tables);

            SaveFileDialog dlg = new SaveFileDialog
            {
                FileName = "Convertisseur.sql",
                Filter = "Fichier SQL (*.sql)|*.sql"
            };
            if (dlg.ShowDialog() == true)
            {
                File.WriteAllText(dlg.FileName, sqlScript, Encoding.UTF8);
                MessageBox.Show("Le script SQL a été généré et sauvegardé avec succès.", "Conversion terminée", MessageBoxButton.OK, MessageBoxImage.Information);
            }
        }

        // Mapping des types AL vers MySQL.
        private static readonly Dictionary<string, string> AlTypeToSqlType = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            // Types "Code"
            { "Code[20]", "varchar(20)" },
            { "Code[10]", "varchar(10)" },
            { "Code[30]", "varchar(30)" },
            { "Code[50]", "varchar(50)" },
            { "Code[100]", "varchar(100)" },
            { "Code[250]", "varchar(250)" },
            { "Code[500]", "varchar(500)" },
            { "Code[1000]", "varchar(1000)" },
            { "Code[2048]", "varchar(2048)" },
            { "Code[Max]", "text" }, // MySQL n'a pas de varchar(max)

            // Types "Text"
            { "Text[10]", "varchar(10)" },
            { "Text[30]", "varchar(30)" },
            { "Text[50]", "varchar(50)" },
            { "Text[100]", "varchar(100)" },
            { "Text[250]", "varchar(250)" },
            { "Text[500]", "varchar(500)" },
            { "Text[1000]", "varchar(1000)" },
            { "Text[2048]", "varchar(2048)" },
            { "Text[Max]", "text" },

            // Types numériques
            { "Integer", "int" },
            { "BigInteger", "bigint" },
            { "Decimal", "decimal" },
            { "Decimal[16,2]", "decimal(16,2)" },
            { "Decimal[20,2]", "decimal(20,2)" },
            { "Decimal[38,20]", "decimal(38,20)" },

            // Types date et heure
            { "Date", "date" },
            { "Time", "time" },
            { "DateTime", "datetime" },

            // Booléen
            { "Boolean", "boolean" },

            // GUID
            { "Guid", "char(36)" },

            // Types BLOB et médias
            { "BLOB", "blob" },
            { "MediaSet", "blob" },
            { "Media", "blob" },
            { "MediaLink", "blob" },
            { "MediaSet[1]", "blob" },
            { "Media[1]", "blob" },
            { "MediaLink[1]", "blob" },
            { "MediaSet[2]", "blob" },
            { "Media[2]", "blob" },
            { "MediaLink[2]", "blob" },
            { "MediaSet[3]", "blob" },
            { "Media[3]", "blob" },
            { "MediaLink[3]", "blob" },
            { "MediaSet[4]", "blob" },
            { "Media[4]", "blob" }
        };

        private string MapALTypeToSQL(string alType)
        {
            alType = alType.Trim();

            if (AlTypeToSqlType.TryGetValue(alType, out string sqlType))
                return sqlType;

            // Traitement dynamique pour les types Code[...] ou Text[...]
            if (alType.StartsWith("Code[", StringComparison.OrdinalIgnoreCase) ||
                alType.StartsWith("Text[", StringComparison.OrdinalIgnoreCase))
            {
                int start = alType.IndexOf('[') + 1;
                int end = alType.IndexOf(']');
                if (start > 0 && end > start)
                {
                    string sizeStr = alType.Substring(start, end - start);
                    if (sizeStr.Equals("Max", StringComparison.OrdinalIgnoreCase))
                        return "text";
                    return $"varchar({sizeStr})";
                }
            }

            // Traitement dynamique pour Decimal[...] (ex: Decimal[16,2])
            if (alType.StartsWith("Decimal[", StringComparison.OrdinalIgnoreCase))
            {
                int start = alType.IndexOf('[') + 1;
                int end = alType.IndexOf(']');
                if (start > 0 && end > start)
                {
                    string parameters = alType.Substring(start, end - start);
                    return $"decimal({parameters})";
                }
            }

            return "text";
        }

        // Méthode de normalisation : supprime espaces et points, puis concatène en capitalisant la première lettre de chaque mot après le premier.
        private string NormalizeName(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
                return input;

            input = input.Replace(".", "");
            var words = input.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            if (words.Length == 0)
                return input;

            string normalized = words[0];
            for (int i = 1; i < words.Length; i++)
            {
                normalized += char.ToUpper(words[i][0]) + words[i].Substring(1);
            }
            return normalized;
        }

        private TableDefinition ParseALFile(string filePath)
        {
            string content = File.ReadAllText(filePath);
            TableDefinition tableDef = new TableDefinition();

            // Extraction de l'en-tête : numéro et nom de la table
            var tableHeaderMatch = Regex.Match(content, @"table\s+(\d+)\s+""([^""]+)""", RegexOptions.IgnoreCase);
            if (!tableHeaderMatch.Success)
                return null;
            tableDef.TableNumber = tableHeaderMatch.Groups[1].Value;
            tableDef.TableName = tableHeaderMatch.Groups[2].Value;

            // Extraction du bloc "fields"
            string fieldsBlock = ExtractBlock(content, "fields");
            if (fieldsBlock != null)
            {
                // On enlève la première et la dernière accolade
                string innerFields = fieldsBlock.Substring(1, fieldsBlock.Length - 2);

                // On découpe en morceaux chacun débutant par "field(" (en préservant le séparateur)
                var fieldChunks = Regex.Split(innerFields, @"(?=field\s*\()");
                foreach (var chunk in fieldChunks)
                {
                    if (string.IsNullOrWhiteSpace(chunk))
                        continue;

                    // On essaie d'extraire la définition du champ, avec un groupe optionnel pour le bloc d'accolades
                    var headerMatch = Regex.Match(chunk,
                        @"field\s*\(\s*\d+\s*;\s*""([^""]+)""\s*;\s*([^\)\{]+)\)\s*(\{.*\})?",
                        RegexOptions.Singleline | RegexOptions.IgnoreCase);

                    if (!headerMatch.Success)
                        continue;

                    ColumnDefinition col = new ColumnDefinition();
                    col.ColumnName = headerMatch.Groups[1].Value.Trim();
                    col.ALType = headerMatch.Groups[2].Value.Trim();
                    col.SQLType = MapALTypeToSQL(col.ALType);

                    // Si le champ possède un bloc (délimité par accolades)
                    if (headerMatch.Groups[3].Success && !string.IsNullOrWhiteSpace(headerMatch.Groups[3].Value))
                    {
                        string block = headerMatch.Groups[3].Value;
                        var tableRelMatch = Regex.Match(block, @"TableRelation\s*=\s*""([^""]+)""", RegexOptions.IgnoreCase);
                        if (tableRelMatch.Success)
                        {
                            string tableRel = tableRelMatch.Groups[1].Value.Trim();
                            // On ignore la relation si la valeur est vide, "undefined" ou si normalisée vaut "NoSeries"
                            if (!string.IsNullOrEmpty(tableRel) &&
                                !tableRel.Equals("undefined", StringComparison.OrdinalIgnoreCase) &&
                                !NormalizeName(tableRel).Equals("NoSeries", StringComparison.OrdinalIgnoreCase))
                            {
                                col.TableRelation = tableRel;
                            }
                        }
                    }

                    tableDef.Columns.Add(col);
                    if (!string.IsNullOrEmpty(col.TableRelation))
                    {
                        // Ajoute une FK pour ce champ
                        ForeignKeyDefinition fk = new ForeignKeyDefinition
                        {
                            ColumnName = col.ColumnName,
                            ReferencedTable = col.TableRelation
                        };
                        tableDef.ForeignKeys.Add(fk);
                    }
                }
            }

            // Extraction des clés primaires (le regex accepte n'importe quel nom de clé)
            string keysBlock = ExtractBlock(content, "keys");
            if (keysBlock != null)
            {
                var pkMatch = Regex.Match(keysBlock, @"key\s*\(\s*[^;]+;\s*([^)]+)\)", RegexOptions.IgnoreCase);
                if (pkMatch.Success)
                {
                    string pkFieldsStr = pkMatch.Groups[1].Value;
                    var pkFieldMatches = Regex.Matches(pkFieldsStr, @"""([^""]+)""");
                    foreach (Match m in pkFieldMatches)
                        tableDef.PrimaryKeys.Add(m.Groups[1].Value.Trim());
                }
            }

            return tableDef;
        }





        private string ExtractBlock(string content, string blockName)
        {
            int index = content.IndexOf(blockName, StringComparison.OrdinalIgnoreCase);
            if (index == -1) return null;
            int startBrace = content.IndexOf('{', index);
            if (startBrace == -1) return null;
            int endBrace = FindMatchingBrace(content, startBrace);
            if (endBrace == -1) return null;
            return content.Substring(startBrace, endBrace - startBrace + 1);
        }

        private int FindMatchingBrace(string text, int startIndex)
        {
            int braceCount = 0;
            for (int i = startIndex; i < text.Length; i++)
            {
                if (text[i] == '{') braceCount++;
                else if (text[i] == '}')
                {
                    braceCount--;
                    if (braceCount == 0)
                        return i;
                }
            }
            return -1;
        }

        private string GenerateSQLScript(List<TableDefinition> tables)
        {
            StringBuilder sb = new StringBuilder();

            // Création des tables avec noms normalisés et backticks pour MySQL
            foreach (var table in tables)
            {
                string normalizedTableName = NormalizeName(table.TableName);
                // Si la table ne contient aucun champ, on ajoute un avertissement et on passe.
                if (!table.Columns.Any())
                {
                    sb.AppendLine($"-- Warning: Table `{normalizedTableName}` est vide et n'a pas été générée.");
                    continue;
                }
                sb.AppendLine($"CREATE TABLE `{normalizedTableName}` (");
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    var col = table.Columns[i];
                    string normalizedColumnName = NormalizeName(col.ColumnName);
                    bool isPK = table.PrimaryKeys.Any(pk => pk.Equals(col.ColumnName, StringComparison.OrdinalIgnoreCase));

                    // Si le champ fait partie d'une clé et que son type est "text", on utilise varchar(255) par défaut.
                    string columnType = col.SQLType;
                    if (isPK && columnType.Equals("text", StringComparison.OrdinalIgnoreCase))
                        columnType = "varchar(255)";

                    sb.Append($"    `{normalizedColumnName}` {columnType} {(isPK ? "NOT NULL" : "NULL")}");
                    sb.AppendLine(i < table.Columns.Count - 1 ? "," : "");
                }
                if (table.PrimaryKeys.Any())
                {
                    string pkCols = string.Join(", ", table.PrimaryKeys.Select(c => $"`{NormalizeName(c)}`"));
                    // On ne tronque que l'identifiant de contrainte
                    string pkIdentifier = $"PK_{normalizedTableName}";
                    pkIdentifier = TruncateIdentifier(pkIdentifier);
                    string pkConstraint = $"CONSTRAINT `{pkIdentifier}` PRIMARY KEY ({pkCols})";
                    sb.AppendLine($",    {pkConstraint}");
                }
                sb.AppendLine(");");
                sb.AppendLine();
            }

            // Génération des clés étrangères avec noms normalisés et backticks
            foreach (var table in tables)
            {
                string normalizedTableName = NormalizeName(table.TableName);
                foreach (var fk in table.ForeignKeys)
                {
                    string normalizedFkColumn = NormalizeName(fk.ColumnName);
                    string normalizedReferencedTable = NormalizeName(fk.ReferencedTable);

                    // Recherche de la table référencée dans nos définitions (normalisée)
                    var referencedTableDef = tables.FirstOrDefault(t => NormalizeName(t.TableName)
                                                        .Equals(normalizedReferencedTable, StringComparison.OrdinalIgnoreCase));
                    if (referencedTableDef == null)
                    {
                        sb.AppendLine($"-- Warning: Table `{normalizedReferencedTable}` non trouvée pour FK sur `{normalizedFkColumn}` dans `{normalizedTableName}`.");
                        continue;
                    }

                    // Utilise la première clé primaire comme colonne référencée
                    string referencedColumn = referencedTableDef.PrimaryKeys.Any() ?
                                              referencedTableDef.PrimaryKeys.First() : "No.";
                    string normalizedReferencedColumn = NormalizeName(referencedColumn);
                    // Si le référencé est de type "text", on le remplace par varchar(255)
                    var pkColumn = referencedTableDef.Columns.FirstOrDefault(c =>
                                    NormalizeName(c.ColumnName).Equals(normalizedReferencedColumn, StringComparison.OrdinalIgnoreCase));
                    if (pkColumn != null && pkColumn.SQLType.Equals("text", StringComparison.OrdinalIgnoreCase))
                        normalizedReferencedColumn = "varchar(255)";

                    // On ne tronque que l'identifiant de contrainte FK
                    string fkIdentifier = $"FK_{normalizedTableName}_{normalizedReferencedTable}_{normalizedFkColumn}";
                    fkIdentifier = TruncateIdentifier(fkIdentifier);

                    sb.AppendLine($"ALTER TABLE `{normalizedTableName}` ADD CONSTRAINT `{fkIdentifier}` " +
                                  $"FOREIGN KEY (`{normalizedFkColumn}`) REFERENCES `{normalizedReferencedTable}` (`{normalizedReferencedColumn}`);");
                }
                sb.AppendLine();
            }
            return sb.ToString();
        }



        // Fonction utilitaire pour tronquer un identifiant à 64 caractères maximum.
        private string TruncateIdentifier(string identifier)
        {
            if (identifier.Length <= 64)
                return identifier;
            return identifier.Substring(0, 64);
        }

        #endregion
    }
}
