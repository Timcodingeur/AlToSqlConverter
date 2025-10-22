using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using IOCompression = System.IO.Compression; // alias pour éviter les conflits
using System.IO.Packaging; // NuGet: System.IO.Packaging (fallback OPC)
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows;
// NuGet: SharpCompress
using SharpCompress.Archives;
using SharpCompress.Common;

namespace frontdruconverteur
{
    public partial class MainWindow : Window
    {
        // ====== Réglages ======
        private const bool GenerateBaseTablesToo = true;

        // Renseigne ici le chemin vers ton .alpackages si besoin
        private const string SymbolsFolderOverride = @"D:\Workspace\Report-122-Project\repport-122\.alpackages";

        // Logs détaillés (met à true si besoin de debug)
        private const bool VerboseSymbolLogging = false;

        // Mode agressif: tenter d'autres JSON plausibles si aucun SymbolReference n'est détecté
        private const bool AggressiveParseAnyJsonInApp = true;

        // Debug 7-Zip: garder les dossiers extraits (sinon ils sont supprimés)
        private const bool KeepExtractedTemp = false;

        // ====== État ======
        private readonly List<string> tableFilePaths = new List<string>();
        private string currentRootFolder = null;

        public MainWindow()
        {
            InitializeComponent();
        }

        #region UI helpers (loader)
        private void SetBusy(bool busy, string text = null)
        {
            Dispatcher.Invoke(() =>
            {
                BusyOverlay.Visibility = busy ? Visibility.Visible : Visibility.Collapsed;
                ConvertButton.IsEnabled = !busy;
                if (!string.IsNullOrWhiteSpace(text))
                    BusyText.Text = text;
            });
        }

        private void UpdateBusy(string text)
        {
            Dispatcher.Invoke(() => BusyText.Text = text);
        }
        #endregion

        #region Drag & Drop
        private void Border_DragEnter(object sender, DragEventArgs e)
        {
            e.Effects = e.Data.GetDataPresent(DataFormats.FileDrop) ? DragDropEffects.Copy : DragDropEffects.None;
        }

        private void Border_Drop(object sender, DragEventArgs e)
        {
            if (!e.Data.GetDataPresent(DataFormats.FileDrop)) return;

            string[] droppedItems = (string[])e.Data.GetData(DataFormats.FileDrop);
            string folderPath = droppedItems[0];

            if (!Directory.Exists(folderPath))
            {
                MessageBox.Show("Le chemin déposé n'est pas un dossier valide.");
                return;
            }

            currentRootFolder = folderPath;

            FileListBox.Items.Clear();
            tableFilePaths.Clear();

            try
            {
                var files = Directory.EnumerateFiles(folderPath, "*", SearchOption.AllDirectories)
                                     .Where(f => string.Equals(Path.GetExtension(f), ".al", StringComparison.OrdinalIgnoreCase));

                foreach (var file in files)
                {
                    try
                    {
                        var parsed = ParseALObject(file);
                        if (parsed != null)
                        {
                            FileListBox.Items.Add(Path.GetFileName(file));
                            tableFilePaths.Add(file);
                        }
                    }
                    catch (Exception ex)
                    {
                        AppendLog($"[AL READ ERROR] {file}: {ex.Message}");
                    }
                }

                if (tableFilePaths.Count == 0)
                    AppendLog("[INFO] Aucun objet AL compris par le parseur dans le dossier déposé.");
            }
            catch (Exception ex)
            {
                MessageBox.Show("Erreur lors de la lecture des fichiers : " + ex.Message);
            }
        }

        private static string StripComments(string input)
        {
            if (string.IsNullOrEmpty(input)) return input;
            string pattern = @"(?s)/\*.*?\*/|//.*?$|^\s*--.*?$";
            return Regex.Replace(input, pattern, "", RegexOptions.Multiline);
        }

        private void AppendLog(string message)
        {
            try
            {
                Console.WriteLine(message);
                System.Diagnostics.Debug.WriteLine(message);
            }
            catch { /* ignore */ }
        }
        #endregion

        #region Conversion (AL -> SQL + SymbolReference)
        private enum AlObjectKind { Table, TableExtension }

        private class ParsedObject
        {
            public AlObjectKind Kind;
            public TableDefinition Table;
            public TableExtensionDefinition TableExtension;
        }

        private class TableDefinition
        {
            public string TableNumber;
            public string TableName;
            public List<ColumnDefinition> Columns { get; set; } = new List<ColumnDefinition>();
            public List<string> PrimaryKeys { get; set; } = new List<string>();
            public List<ForeignKeyDefinition> ForeignKeys { get; set; } = new List<ForeignKeyDefinition>();
        }

        private class TableExtensionDefinition
        {
            public string ExtensionNumber;
            public string ExtensionName;
            public string BaseTableName;
            public List<ColumnDefinition> Columns { get; set; } = new List<ColumnDefinition>();
            public List<ForeignKeyDefinition> ForeignKeys { get; set; } = new List<ForeignKeyDefinition>();
        }

        private class ColumnDefinition
        {
            public string ColumnName;
            public string ALType;   // ex: "Code[20]"
            public string SQLType;  // ex: "varchar(20)"
            public string TableRelation;
        }

        private class ForeignKeyDefinition
        {
            public string ColumnName;
            public string ReferencedTable;
        }

        private async void ConvertButton_Click(object sender, RoutedEventArgs e)
        {
            if (tableFilePaths.Count == 0)
            {
                MessageBox.Show("Aucun fichier AL de table ou tableextension détecté.");
                return;
            }

            SetBusy(true, "Analyse des objets AL...");

            try
            {
                // 1) Parsing AL (BG)
                var parsedObjects = await Task.Run(() =>
                {
                    var list = new List<ParsedObject>();
                    foreach (var file in tableFilePaths)
                    {
                        var obj = ParseALObject(file);
                        if (obj != null) list.Add(obj);
                    }
                    return list;
                });

                if (parsedObjects.Count == 0)
                {
                    MessageBox.Show("Aucune définition de table ou d'extension valide n'a été trouvée.");
                    SetBusy(false);
                    return;
                }

                SetBusy(true, "Recherche des symboles des .app...");

                // 2) Racines des symboles (optim: si override existe, on ne scanne que lui)
                var roots = CollectSymbolSearchRoots(currentRootFolder, SymbolsFolderOverride);

                // 3) Chargement symboles (BG)
                var externalBaseTables = await Task.Run(() =>
                    LoadExternalBaseTables(roots, status => UpdateBusy(status))
                );

                // 4) Post-traitement
                var baseTables = parsedObjects.Where(o => o.Kind == AlObjectKind.Table).Select(o => o.Table).ToList();
                var extensions = parsedObjects.Where(o => o.Kind == AlObjectKind.TableExtension).Select(o => o.TableExtension).ToList();

                var baseTableMap = baseTables
                    .GroupBy(t => NormalizeName(t.TableName), StringComparer.OrdinalIgnoreCase)
                    .ToDictionary(g => g.Key, g => MergeTables(g.ToList()), StringComparer.OrdinalIgnoreCase);

                var flattenedTables = BuildFlattenedTables(baseTableMap, externalBaseTables, extensions);

                foreach (var ext in extensions)
                {
                    var key = NormalizeName(ext.BaseTableName);
                    bool foundInternal = baseTableMap.ContainsKey(key);
                    bool foundExternal = externalBaseTables.ContainsKey(key);
                    AppendLog($"[FLATTEN] Base '{ext.BaseTableName}' -> internal:{foundInternal} external:{foundExternal}");
                }

                SetBusy(true, "Génération du script SQL...");

                string sqlScript = await Task.Run(() => GenerateSQLScript(baseTables, flattenedTables));

                var dlg = new SaveFileDialog
                {
                    FileName = "Convertisseur.sql",
                    Filter = "Fichier SQL (*.sql)|*.sql"
                };
                if (dlg.ShowDialog() == true)
                {
                    File.WriteAllText(dlg.FileName, sqlScript, Encoding.UTF8);
                    MessageBox.Show("Le script SQL a été généré.", "OK", MessageBoxButton.OK, MessageBoxImage.Information);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show("Erreur: " + ex.Message, "Erreur", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                SetBusy(false);
            }
        }

        private TableDefinition MergeTables(List<TableDefinition> defs)
        {
            var result = new TableDefinition
            {
                TableNumber = defs.FirstOrDefault()?.TableNumber ?? "",
                TableName = defs.FirstOrDefault()?.TableName ?? ""
            };

            foreach (var t in defs)
            {
                foreach (var col in t.Columns)
                {
                    if (!result.Columns.Any(c => NormalizeName(c.ColumnName).Equals(NormalizeName(col.ColumnName), StringComparison.OrdinalIgnoreCase)))
                        result.Columns.Add(col);
                }
                foreach (var pk in t.PrimaryKeys)
                {
                    if (!result.PrimaryKeys.Any(p => p.Equals(pk, StringComparison.OrdinalIgnoreCase)))
                        result.PrimaryKeys.Add(pk);
                }
                foreach (var fk in t.ForeignKeys)
                {
                    result.ForeignKeys.Add(fk);
                }
            }

            return result;
        }

        private bool IsMicrosoftTable(TableDefinition baseTable)
        {
            if (baseTable == null) return true;
            if (int.TryParse(baseTable.TableNumber, out var id))
                return id > 0 && id < 50000;
            return true;
        }

        private string PrefixMicrosoft(string name)
        {
            if (string.IsNullOrWhiteSpace(name)) return name;
            return name.StartsWith("M_", StringComparison.OrdinalIgnoreCase) ? name : "M_" + name;
        }

        private List<TableDefinition> BuildFlattenedTables(
            Dictionary<string, TableDefinition> internalBaseTableMap,
            Dictionary<string, TableDefinition> externalBaseTableMap,
            List<TableExtensionDefinition> extensions)
        {
            var flattened = new List<TableDefinition>();

            foreach (var ext in extensions)
            {
                string baseKey = NormalizeName(ext.BaseTableName);
                internalBaseTableMap.TryGetValue(baseKey, out var baseTable);
                if (baseTable == null)
                    externalBaseTableMap.TryGetValue(baseKey, out baseTable);

                bool baseIsMicrosoft = IsMicrosoftTable(baseTable);

                var flat = new TableDefinition
                {
                    TableNumber = ext.ExtensionNumber ?? "",
                    TableName = ext.ExtensionName
                };

                // Colonnes de la base (préfixe M_ si Microsoft)
                if (baseTable != null)
                {
                    foreach (var c in baseTable.Columns)
                    {
                        var cloned = new ColumnDefinition
                        {
                            ColumnName = baseIsMicrosoft ? PrefixMicrosoft(c.ColumnName) : c.ColumnName,
                            ALType = c.ALType,
                            SQLType = ResolveSqlType(c), // garanti de donner un type exploitable
                            TableRelation = c.TableRelation
                        };
                        flat.Columns.Add(cloned);
                    }
                }
                else
                {
                    AppendLog($"[WARN] Base introuvable pour extension '{ext.ExtensionName}' extends '{ext.BaseTableName}'. Seules les colonnes de l’extension seront présentes.");
                }

                // Colonnes de l’extension (sans doublons)
                foreach (var c in ext.Columns)
                {
                    if (!flat.Columns.Any(x => NormalizeName(x.ColumnName).Equals(NormalizeName(c.ColumnName), StringComparison.OrdinalIgnoreCase)))
                        flat.Columns.Add(new ColumnDefinition
                        {
                            ColumnName = c.ColumnName,
                            ALType = c.ALType,
                            SQLType = ResolveSqlType(c),
                            TableRelation = c.TableRelation
                        });
                }

                // PK = PK(base) (préfixée si Microsoft)
                if (baseTable != null && baseTable.PrimaryKeys.Any())
                {
                    foreach (var pk in baseTable.PrimaryKeys)
                        flat.PrimaryKeys.Add(baseIsMicrosoft ? PrefixMicrosoft(pk) : pk);
                }

                // FKs = FKs(base) (préfixées si Microsoft) + FKs(ext)
                if (baseTable != null)
                {
                    foreach (var fk in baseTable.ForeignKeys)
                    {
                        flat.ForeignKeys.Add(new ForeignKeyDefinition
                        {
                            ColumnName = baseIsMicrosoft ? PrefixMicrosoft(fk.ColumnName) : fk.ColumnName,
                            ReferencedTable = fk.ReferencedTable
                        });
                    }
                }
                foreach (var fk in ext.ForeignKeys)
                    flat.ForeignKeys.Add(fk);

                flattened.Add(flat);
            }

            return flattened;
        }

        private List<string> CollectSymbolSearchRoots(string rootFolder, string overridePath)
        {
            var roots = new List<string>();

            // OPTIM: si override existe, on se limite à ce dossier
            if (!string.IsNullOrWhiteSpace(overridePath) && Directory.Exists(overridePath))
            {
                roots.Add(overridePath);
                AppendLog($"[SYMBOL SEARCH ROOTS]\n  - {overridePath}");
                return roots;
            }

            if (!string.IsNullOrWhiteSpace(rootFolder))
            {
                var cur = new DirectoryInfo(rootFolder);
                while (cur != null && cur.Parent != null)
                {
                    roots.Add(cur.FullName);
                    var alpkg = Path.Combine(cur.FullName, ".alpackages");
                    if (Directory.Exists(alpkg))
                        roots.Add(alpkg);
                    cur = cur.Parent;
                }
            }

            roots = roots.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            AppendLog($"[SYMBOL SEARCH ROOTS]\n  - {string.Join("\n  - ", roots)}");
            return roots;
        }

        private Dictionary<string, TableDefinition> LoadExternalBaseTables(List<string> searchRoots, Action<string> reportStatus = null)
        {
            var result = new Dictionary<string, TableDefinition>(StringComparer.OrdinalIgnoreCase);
            int appsSeen = 0, jsonSeen = 0, totalSymbolEntries = 0;
            var processedApps = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var root in searchRoots)
            {
                if (!Directory.Exists(root)) continue;

                try
                {
                    // JSON libres sur disque
                    foreach (var symJson in SafeEnumFiles(root, "SymbolReference.json"))
                    {
                        jsonSeen++;
                        try
                        {
                            reportStatus?.Invoke($"Lecture symboles JSON...\n{Path.GetFileName(symJson)}");
                            using var fs = File.OpenRead(symJson);
                            int before = result.Count;
                            ParseSymbolReferenceJson(fs, result);
                            int after = result.Count;
                            AppendLog($"[SYMBOL] JSON: {symJson} | +{after - before} tables");
                        }
                        catch (Exception ex)
                        {
                            AppendLog($"[SYMBOL JSON ERROR] {symJson}: {ex.Message}");
                        }
                    }

                    // .app (une seule fois chacun)
                    foreach (var app in SafeEnumFiles(root, "*.app"))
                    {
                        if (!processedApps.Add(app)) continue;

                        appsSeen++;
                        int addedFromThisApp = 0;

                        try
                        {
                            reportStatus?.Invoke($"Analyse du paquet...\n{Path.GetFileName(app)}");

                            // 1) ZIP standard
                            try
                            {
                                using var fs = File.Open(app, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                                using var za = new IOCompression.ZipArchive(fs, IOCompression.ZipArchiveMode.Read, leaveOpen: false);

                                foreach (var streamInfo in OpenAllSymbolReferenceStreamsFromZip(za, app))
                                {
                                    using var sr = streamInfo.Stream;
                                    int before = result.Count;
                                    ParseSymbolReferenceJson(sr, result);
                                    int after = result.Count;
                                    int delta = after - before;
                                    totalSymbolEntries++;
                                    addedFromThisApp += delta;
                                    if (VerboseSymbolLogging) AppendLog($"[APP SYMBOL ZIP] {app} :: {streamInfo.EntryName} | +{delta} tables");
                                }
                            }
                            catch (InvalidDataException zex)
                            {
                                if (VerboseSymbolLogging) AppendLog($"[APP READ ERROR ZIP] {app}: {zex.Message} -> fallback OPC/Sharp/7z");
                            }

                            // 2) OPC fallback
                            try
                            {
                                foreach (var streamInfo in OpenAllSymbolReferenceStreamsFrom7ZipFast(app))
                                {
                                    using var sr = streamInfo.Stream;
                                    int before = result.Count;
                                    ParseSymbolReferenceJson(sr, result);
                                    int after = result.Count;
                                    int delta = after - before;
                                    totalSymbolEntries++;
                                    addedFromThisApp += delta;
                                    if (VerboseSymbolLogging) AppendLog($"[APP SYMBOL OPC] {app} :: {streamInfo.EntryName} | +{delta} tables");
                                }
                            }
                            catch (Exception pex)
                            {
                                if (VerboseSymbolLogging) AppendLog($"[APP READ ERROR OPC] {app}: {pex.Message} -> fallback Sharp/7z");
                            }

                            // 3) SharpCompress fallback
                            try
                            {
                                foreach (var streamInfo in OpenAllSymbolReferenceStreamsFromSharpZip(app))
                                {
                                    using var sr = streamInfo.Stream;
                                    int before = result.Count;
                                    ParseSymbolReferenceJson(sr, result);
                                    int after = result.Count;
                                    int delta = after - before;
                                    totalSymbolEntries++;
                                    addedFromThisApp += delta;
                                    if (VerboseSymbolLogging) AppendLog($"[APP SYMBOL SHARP] {app} :: {streamInfo.EntryName} | +{delta} tables");
                                }
                            }
                            catch (Exception sex)
                            {
                                if (VerboseSymbolLogging) AppendLog($"[APP READ ERROR SHARP] {app}: {sex.Message} -> fallback 7z");
                            }

                            // 4) 7‑Zip FAST: lister puis extraire uniquement SymbolReference.*
                            try
                            {
                                foreach (var streamInfo in OpenAllSymbolReferenceStreamsFrom7ZipFast(app, reportStatus))
                                {
                                    using var sr = streamInfo.Stream;
                                    int before = result.Count;
                                    ParseSymbolReferenceJson(sr, result);
                                    int after = result.Count;
                                    int delta = after - before;
                                    totalSymbolEntries++;
                                    addedFromThisApp += delta;
                                    if (VerboseSymbolLogging) AppendLog($"[APP SYMBOL 7Z] {app} :: {streamInfo.EntryName} | +{delta} tables");
                                }
                            }
                            catch (Exception zex2)
                            {
                                if (VerboseSymbolLogging) AppendLog($"[APP READ ERROR 7Z] {app}: {zex2.Message}");
                            }

                            if (VerboseSymbolLogging && addedFromThisApp == 0)
                                AppendLog($"[APP SYMBOL] {app} -> 0 tables (aucune entrée symbole reconnue)");
                        }
                        catch (Exception ex)
                        {
                            AppendLog($"[APP READ ERROR] {app}: {ex.Message}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    AppendLog($"[SYMBOL ROOT ERROR] {root}: {ex.Message}");
                }
            }

            AppendLog($"[SYMBOL SUMMARY] JSON files: {jsonSeen}, APPs: {appsSeen}, Symbol entries tried: {totalSymbolEntries}, Tables loaded (unique): {result.Count}");
            return result;
        }

        private sealed class SymbolStreamInfo : IDisposable
        {
            public Stream Stream { get; set; }
            public string EntryName { get; set; }
            public void Dispose() => Stream?.Dispose();
        }

        // ZIP (.json / .gz / .br + fallback)
        private IEnumerable<SymbolStreamInfo> OpenAllSymbolReferenceStreamsFromZip(IOCompression.ZipArchive za, string appPathForLog)
        {
            foreach (var e in za.Entries.Where(e => e.FullName.EndsWith("SymbolReference.json", StringComparison.OrdinalIgnoreCase)))
            {
                if (VerboseSymbolLogging) AppendLog($"[APP ENTRY ZIP] {appPathForLog} :: {e.FullName}");
                yield return new SymbolStreamInfo { Stream = e.Open(), EntryName = e.FullName };
            }

            foreach (var e in za.Entries.Where(e => e.FullName.EndsWith("SymbolReference.json.gz", StringComparison.OrdinalIgnoreCase)))
            {
                if (VerboseSymbolLogging) AppendLog($"[APP ENTRY ZIP GZ] {appPathForLog} :: {e.FullName}");
                var ms = new MemoryStream();
                using (var gz = new IOCompression.GZipStream(e.Open(), IOCompression.CompressionMode.Decompress, leaveOpen: false))
                    gz.CopyTo(ms);
                ms.Position = 0;
                yield return new SymbolStreamInfo { Stream = ms, EntryName = e.FullName };
            }

            foreach (var e in za.Entries.Where(e => e.FullName.EndsWith("SymbolReference.json.br", StringComparison.OrdinalIgnoreCase)))
            {
                if (VerboseSymbolLogging) AppendLog($"[APP ENTRY ZIP BR] {appPathForLog} :: {e.FullName}");
                var ms = new MemoryStream();
                using (var br = new IOCompression.BrotliStream(e.Open(), IOCompression.CompressionMode.Decompress, leaveOpen: false))
                    br.CopyTo(ms);
                ms.Position = 0;
                yield return new SymbolStreamInfo { Stream = ms, EntryName = e.FullName };
            }

            foreach (var e in za.Entries.Where(e => e.FullName.IndexOf("symbolreference", StringComparison.OrdinalIgnoreCase) >= 0))
            {
                if (VerboseSymbolLogging) AppendLog($"[APP ENTRY ZIP ANY] {appPathForLog} :: {e.FullName}");
                if (e.FullName.EndsWith(".gz", StringComparison.OrdinalIgnoreCase))
                {
                    var ms = new MemoryStream();
                    using (var gz = new IOCompression.GZipStream(e.Open(), IOCompression.CompressionMode.Decompress, leaveOpen: false))
                        gz.CopyTo(ms);
                    ms.Position = 0;
                    yield return new SymbolStreamInfo { Stream = ms, EntryName = e.FullName };
                }
                else if (e.FullName.EndsWith(".br", StringComparison.OrdinalIgnoreCase))
                {
                    var ms = new MemoryStream();
                    using (var br = new IOCompression.BrotliStream(e.Open(), IOCompression.CompressionMode.Decompress, leaveOpen: false))
                        br.CopyTo(ms);
                    ms.Position = 0;
                    yield return new SymbolStreamInfo { Stream = ms, EntryName = e.FullName };
                }
                else if (e.FullName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
                {
                    yield return new SymbolStreamInfo { Stream = e.Open(), EntryName = e.FullName };
                }
            }

            if (AggressiveParseAnyJsonInApp)
            {
                var otherJsons = za.Entries.Where(e =>
                        e.FullName.EndsWith(".json", StringComparison.OrdinalIgnoreCase) ||
                        e.FullName.EndsWith(".json.gz", StringComparison.OrdinalIgnoreCase) ||
                        e.FullName.EndsWith(".json.br", StringComparison.OrdinalIgnoreCase))
                    .Where(e => e.FullName.IndexOf("symbol", StringComparison.OrdinalIgnoreCase) >= 0
                             || e.FullName.IndexOf("reference", StringComparison.OrdinalIgnoreCase) >= 0
                             || e.FullName.IndexOf("metadata", StringComparison.OrdinalIgnoreCase) >= 0
                             || e.FullName.IndexOf("objects", StringComparison.OrdinalIgnoreCase) >= 0)
                    .Take(10)
                    .ToList();

                foreach (var e in otherJsons)
                {
                    if (VerboseSymbolLogging) AppendLog($"[APP ENTRY ZIP FALLBACK] {appPathForLog} :: {e.FullName}");
                    MemoryStream ms = new MemoryStream();
                    using (var src = e.Open())
                    {
                        if (e.FullName.EndsWith(".gz", StringComparison.OrdinalIgnoreCase))
                        {
                            using var gz = new IOCompression.GZipStream(src, IOCompression.CompressionMode.Decompress, leaveOpen: false);
                            gz.CopyTo(ms);
                        }
                        else if (e.FullName.EndsWith(".br", StringComparison.OrdinalIgnoreCase))
                        {
                            using var br = new IOCompression.BrotliStream(src, IOCompression.CompressionMode.Decompress, leaveOpen: false);
                            br.CopyTo(ms);
                        }
                        else
                        {
                            src.CopyTo(ms);
                        }
                    }
                    ms.Position = 0;
                    yield return new SymbolStreamInfo { Stream = ms, EntryName = e.FullName };
                }
            }
        }

        // SharpCompress (.zip exotiques)
        private IEnumerable<SymbolStreamInfo> OpenAllSymbolReferenceStreamsFromSharpZip(string appPath)
        {
            using var stream = File.Open(appPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var arc = ArchiveFactory.Open(stream);

            foreach (var entry in arc.Entries.Where(e => !e.IsDirectory && e.Key.EndsWith("SymbolReference.json", StringComparison.OrdinalIgnoreCase)))
            {
                if (VerboseSymbolLogging) AppendLog($"[APP ENTRY SHARP] {appPath} :: {entry.Key}");
                var ms = new MemoryStream();
                using (var es = entry.OpenEntryStream()) es.CopyTo(ms);
                ms.Position = 0;
                yield return new SymbolStreamInfo { Stream = ms, EntryName = entry.Key };
            }

            foreach (var entry in arc.Entries.Where(e => !e.IsDirectory && e.Key.EndsWith("SymbolReference.json.gz", StringComparison.OrdinalIgnoreCase)))
            {
                if (VerboseSymbolLogging) AppendLog($"[APP ENTRY SHARP GZ] {appPath} :: {entry.Key}");
                var decompressed = new MemoryStream();
                using (var es = entry.OpenEntryStream())
                using (var gz = new IOCompression.GZipStream(es, IOCompression.CompressionMode.Decompress, leaveOpen: false))
                    gz.CopyTo(decompressed);
                decompressed.Position = 0;
                yield return new SymbolStreamInfo { Stream = decompressed, EntryName = entry.Key };
            }

            foreach (var entry in arc.Entries.Where(e => !e.IsDirectory && e.Key.EndsWith("SymbolReference.json.br", StringComparison.OrdinalIgnoreCase)))
            {
                if (VerboseSymbolLogging) AppendLog($"[APP ENTRY SHARP BR] {appPath} :: {entry.Key}");
                var decompressed = new MemoryStream();
                using (var es = entry.OpenEntryStream())
                using (var br = new IOCompression.BrotliStream(es, IOCompression.CompressionMode.Decompress, leaveOpen: false))
                    br.CopyTo(decompressed);
                decompressed.Position = 0;
                yield return new SymbolStreamInfo { Stream = decompressed, EntryName = entry.Key };
            }

            foreach (var entry in arc.Entries.Where(e => !e.IsDirectory && e.Key.IndexOf("symbolreference", StringComparison.OrdinalIgnoreCase) >= 0))
            {
                if (VerboseSymbolLogging) AppendLog($"[APP ENTRY SHARP ANY] {appPath} :: {entry.Key}");
                var ms = new MemoryStream();
                using (var es = entry.OpenEntryStream())
                {
                    if (entry.Key.EndsWith(".gz", StringComparison.OrdinalIgnoreCase))
                    {
                        using var gz = new IOCompression.GZipStream(es, IOCompression.CompressionMode.Decompress, leaveOpen: false);
                        gz.CopyTo(ms);
                    }
                    else if (entry.Key.EndsWith(".br", StringComparison.OrdinalIgnoreCase))
                    {
                        using var br = new IOCompression.BrotliStream(es, IOCompression.CompressionMode.Decompress, leaveOpen: false);
                        br.CopyTo(ms);
                    }
                    else
                    {
                        es.CopyTo(ms);
                    }
                }
                ms.Position = 0;
                yield return new SymbolStreamInfo { Stream = ms, EntryName = entry.Key };
            }

            if (AggressiveParseAnyJsonInApp)
            {
                var other = arc.Entries.Where(e =>
                        !e.IsDirectory && (e.Key.EndsWith(".json", StringComparison.OrdinalIgnoreCase)
                                        || e.Key.EndsWith(".json.gz", StringComparison.OrdinalIgnoreCase)
                                        || e.Key.EndsWith(".json.br", StringComparison.OrdinalIgnoreCase)))
                    .Where(e => e.Key.IndexOf("symbol", StringComparison.OrdinalIgnoreCase) >= 0
                             || e.Key.IndexOf("reference", StringComparison.OrdinalIgnoreCase) >= 0
                             || e.Key.IndexOf("metadata", StringComparison.OrdinalIgnoreCase) >= 0
                             || e.Key.IndexOf("objects", StringComparison.OrdinalIgnoreCase) >= 0)
                    .Take(10)
                    .ToList();

                foreach (var entry in other)
                {
                    if (VerboseSymbolLogging) AppendLog($"[APP ENTRY SHARP FALLBACK] {appPath} :: {entry.Key}");
                    var ms = new MemoryStream();
                    using (var es = entry.OpenEntryStream())
                    {
                        if (entry.Key.EndsWith(".gz", StringComparison.OrdinalIgnoreCase))
                        {
                            using var gz = new IOCompression.GZipStream(es, IOCompression.CompressionMode.Decompress, leaveOpen: false);
                            gz.CopyTo(ms);
                        }
                        else if (entry.Key.EndsWith(".br", StringComparison.OrdinalIgnoreCase))
                        {
                            using var br = new IOCompression.BrotliStream(es, IOCompression.CompressionMode.Decompress, leaveOpen: false);
                            br.CopyTo(ms);
                        }
                        else
                        {
                            es.CopyTo(ms);
                        }
                    }
                    ms.Position = 0;
                    yield return new SymbolStreamInfo { Stream = ms, EntryName = entry.Key };
                }
            }
        }

        // 7‑Zip FAST: liste puis extrait uniquement les SymbolReference.*
        private IEnumerable<SymbolStreamInfo> OpenAllSymbolReferenceStreamsFrom7ZipFast(string appPath, Action<string> reportStatus = null)
        {
            string sevenZip = Find7ZipExe();
            if (string.IsNullOrEmpty(sevenZip))
                throw new InvalidOperationException("7z.exe introuvable. Installe 7‑Zip (https://www.7-zip.org/) ou mets 7z.exe dans le PATH.");

            reportStatus?.Invoke($"Liste du contenu...\n{Path.GetFileName(appPath)}");
            var listArgs = $"l -slt -ba \"{appPath}\"";
            var listOutput = RunProcessAndGetStdOut(sevenZip, listArgs, 60000);

            var entryNames = new List<string>();
            using (var sr = new StringReader(listOutput))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    if (line.StartsWith("Path = ", StringComparison.OrdinalIgnoreCase))
                    {
                        var path = line.Substring(7).Trim();
                        if (!string.IsNullOrEmpty(path) &&
                            path.IndexOf("symbolreference", StringComparison.OrdinalIgnoreCase) >= 0 &&
                            (path.EndsWith(".json", StringComparison.OrdinalIgnoreCase) ||
                             path.EndsWith(".json.gz", StringComparison.OrdinalIgnoreCase) ||
                             path.EndsWith(".json.br", StringComparison.OrdinalIgnoreCase)))
                        {
                            entryNames.Add(path);
                        }
                    }
                }
            }

            if (entryNames.Count == 0 && AggressiveParseAnyJsonInApp)
            {
                using (var sr = new StringReader(listOutput))
                {
                    string line;
                    while ((line = sr.ReadLine()) != null)
                    {
                        if (line.StartsWith("Path = ", StringComparison.OrdinalIgnoreCase))
                        {
                            var path = line.Substring(7).Trim();
                            if (!string.IsNullOrEmpty(path) &&
                                (path.EndsWith(".json", StringComparison.OrdinalIgnoreCase) ||
                                 path.EndsWith(".json.gz", StringComparison.OrdinalIgnoreCase) ||
                                 path.EndsWith(".json.br", StringComparison.OrdinalIgnoreCase)) &&
                                (Path.GetFileName(path).IndexOf("symbol", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                 Path.GetFileName(path).IndexOf("reference", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                 Path.GetFileName(path).IndexOf("metadata", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                 Path.GetFileName(path).IndexOf("objects", StringComparison.OrdinalIgnoreCase) >= 0))
                            {
                                entryNames.Add(path);
                                if (entryNames.Count >= 10) break;
                            }
                        }
                    }
                }
            }

            if (entryNames.Count == 0) yield break;

            string tempRoot = Path.Combine(Path.GetTempPath(), "ALAppExtract", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(tempRoot);

            try
            {
                foreach (var name in entryNames.Distinct(StringComparer.OrdinalIgnoreCase))
                {
                    reportStatus?.Invoke($"Extraction ciblée...\n{Path.GetFileName(appPath)}");
                    string extractArgs = $"x -y -o\"{tempRoot}\" \"{appPath}\" \"{name}\"";
                    RunProcessAndGetStdOut(sevenZip, extractArgs, 60000);

                    string extractedPath = Path.Combine(tempRoot, name.Replace('/', Path.DirectorySeparatorChar));
                    if (!File.Exists(extractedPath))
                    {
                        var fileNameOnly = Path.GetFileName(name);
                        var alt = Directory.EnumerateFiles(tempRoot, fileNameOnly, SearchOption.AllDirectories).FirstOrDefault();
                        if (alt != null) extractedPath = alt;
                    }

                    if (!File.Exists(extractedPath))
                    {
                        if (VerboseSymbolLogging) AppendLog($"[7Z EXTRACT MISS] {name}");
                        continue;
                    }

                    Stream toYield;
                    if (extractedPath.EndsWith(".gz", StringComparison.OrdinalIgnoreCase))
                    {
                        var ms = new MemoryStream();
                        using (var fs = File.OpenRead(extractedPath))
                        using (var gz = new IOCompression.GZipStream(fs, IOCompression.CompressionMode.Decompress, leaveOpen: false))
                            gz.CopyTo(ms);
                        ms.Position = 0;
                        toYield = ms;
                    }
                    else if (extractedPath.EndsWith(".br", StringComparison.OrdinalIgnoreCase))
                    {
                        var ms = new MemoryStream();
                        using (var fs = File.OpenRead(extractedPath))
                        using (var br = new IOCompression.BrotliStream(fs, IOCompression.CompressionMode.Decompress, leaveOpen: false))
                            br.CopyTo(ms);
                        ms.Position = 0;
                        toYield = ms;
                    }
                    else
                    {
                        toYield = File.OpenRead(extractedPath); // JSON direct
                    }

                    var relName = extractedPath.Substring(tempRoot.Length).TrimStart(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
                    if (VerboseSymbolLogging) AppendLog($"[APP ENTRY 7Z] {appPath} :: {relName}");
                    yield return new SymbolStreamInfo { Stream = toYield, EntryName = relName };
                }
            }
            finally
            {
                if (!KeepExtractedTemp)
                {
                    try { Directory.Delete(tempRoot, recursive: true); } catch { /* ignore */ }
                }
                else
                {
                    AppendLog($"[DEBUG] Dossier 7‑Zip conservé: {tempRoot}");
                }
            }
        }

        // Localise 7z.exe (Program Files x64/x86 ou PATH)
        private string Find7ZipExe()
        {
            var candidates = new[]
            {
                Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles), "7-Zip", "7z.exe"),
                Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFilesX86), "7-Zip", "7z.exe"),
                "7z.exe"
            };
            foreach (var c in candidates)
            {
                try
                {
                    if (c.Equals("7z.exe", StringComparison.OrdinalIgnoreCase))
                    {
                        var psi = new ProcessStartInfo
                        {
                            FileName = c,
                            Arguments = "--help",
                            RedirectStandardOutput = true,
                            RedirectStandardError = true,
                            UseShellExecute = false,
                            CreateNoWindow = true
                        };
                        using var p = Process.Start(psi);
                        if (p != null)
                        {
                            p.WaitForExit(2000);
                            if (!p.HasExited) try { p.Kill(); } catch { }
                            return c;
                        }
                    }
                    else if (File.Exists(c))
                    {
                        return c;
                    }
                }
                catch { /* ignore */ }
            }
            return null;
        }

        // Exécute un process et retourne stdout (UTF-8)
        private string RunProcessAndGetStdOut(string exe, string args, int timeoutMs)
        {
            var psi = new ProcessStartInfo
            {
                FileName = exe,
                Arguments = args,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
                StandardOutputEncoding = Encoding.UTF8,
                StandardErrorEncoding = Encoding.UTF8
            };
            using var p = new Process { StartInfo = psi };
            var sbOut = new StringBuilder();
            var sbErr = new StringBuilder();
            p.OutputDataReceived += (s, e) => { if (e.Data != null) sbOut.AppendLine(e.Data); };
            p.ErrorDataReceived += (s, e) => { if (e.Data != null) sbErr.AppendLine(e.Data); };
            if (!p.Start()) throw new InvalidOperationException($"Impossible de démarrer {exe}");
            p.BeginOutputReadLine();
            p.BeginErrorReadLine();
            if (!p.WaitForExit(timeoutMs))
            {
                try { p.Kill(); } catch { }
                throw new TimeoutException($"{exe} a dépassé le délai ({timeoutMs} ms)");
            }
            if (p.ExitCode != 0 && p.ExitCode != 1) // 1 = warnings acceptables
                throw new InvalidOperationException($"{exe} a échoué ({p.ExitCode}): {sbErr}");
            return sbOut.ToString();
        }

        private IEnumerable<string> SafeEnumFiles(string root, string pattern)
        {
            try { return Directory.EnumerateFiles(root, pattern, SearchOption.AllDirectories); }
            catch { return Array.Empty<string>(); }
        }

        private IEnumerable<JsonElement> EnumerateTableLikeObjects(JsonElement root)
        {
            var stack = new Stack<JsonElement>();
            stack.Push(root);

            while (stack.Count > 0)
            {
                var node = stack.Pop();

                if (node.ValueKind == JsonValueKind.Object)
                {
                    bool isTable =
                        (TryGetProperty(node, "Type", out var tp) && tp.ValueKind == JsonValueKind.String && string.Equals(tp.GetString(), "Table", StringComparison.OrdinalIgnoreCase))
                     || (TryGetProperty(node, "Kind", out var kd) && kd.ValueKind == JsonValueKind.String && string.Equals(kd.GetString(), "Table", StringComparison.OrdinalIgnoreCase))
                     || (TryGetProperty(node, "ObjectType", out var ot) && ot.ValueKind == JsonValueKind.String && string.Equals(ot.GetString(), "Table", StringComparison.OrdinalIgnoreCase))
                     || (TryGetProperty(node, "ALObjectType", out var at) && at.ValueKind == JsonValueKind.String && string.Equals(at.GetString(), "Table", StringComparison.OrdinalIgnoreCase))
                     || (TryGetProperty(node, "SymbolKind", out var sk) && sk.ValueKind == JsonValueKind.String && string.Equals(sk.GetString(), "Table", StringComparison.OrdinalIgnoreCase))
                     || (TryGetProperty(node, "Fields", out var fields) && fields.ValueKind == JsonValueKind.Array);

                    if (isTable)
                        yield return node;

                    foreach (var prop in node.EnumerateObject())
                    {
                        if (prop.Value.ValueKind == JsonValueKind.Array || prop.Value.ValueKind == JsonValueKind.Object)
                            stack.Push(prop.Value);
                    }
                }
                else if (node.ValueKind == JsonValueKind.Array)
                {
                    foreach (var item in node.EnumerateArray())
                    {
                        if (item.ValueKind == JsonValueKind.Array || item.ValueKind == JsonValueKind.Object)
                            stack.Push(item);
                    }
                }
            }
        }

        private void ParseSymbolReferenceJson(Stream jsonStream, Dictionary<string, TableDefinition> tables)
        {
            var options = new JsonDocumentOptions
            {
                AllowTrailingCommas = true,
                CommentHandling = JsonCommentHandling.Skip,
                MaxDepth = 0
            };

            using var doc = JsonDocument.Parse(jsonStream, options);
            var root = doc.RootElement;

            foreach (var tableObj in EnumerateTableLikeObjects(root))
            {
                try
                {
                    string name = GetString(tableObj, "Name");
                    if (string.IsNullOrWhiteSpace(name)) continue;

                    string id = "";
                    if (TryGetProperty(tableObj, "Id", out var idProp))
                    {
                        if (idProp.ValueKind == JsonValueKind.Number && idProp.TryGetInt32(out var idInt)) id = idInt.ToString();
                        else id = idProp.ToString();
                    }

                    var tableDef = new TableDefinition
                    {
                        TableNumber = id ?? "",
                        TableName = name
                    };

                    if (TryGetProperty(tableObj, "Fields", out var fieldsElem) && fieldsElem.ValueKind == JsonValueKind.Array)
                        ParseSymbolFields(fieldsElem, tableDef);
                    else if (TryGetProperty(tableObj, "Members", out var membersElem) && membersElem.ValueKind == JsonValueKind.Array)
                        ParseSymbolFields(membersElem, tableDef);

                    bool pkSet = false;
                    if (TryGetProperty(tableObj, "PrimaryKey", out var pk) && pk.ValueKind == JsonValueKind.Object)
                    {
                        if (TryGetProperty(pk, "KeyFields", out var kf) && kf.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var k in kf.EnumerateArray())
                            {
                                string kname = GetString(k, "Name");
                                if (!string.IsNullOrWhiteSpace(kname)) tableDef.PrimaryKeys.Add(kname);
                            }
                            pkSet = tableDef.PrimaryKeys.Count > 0;
                        }
                    }
                    if (!pkSet && TryGetProperty(tableObj, "Keys", out var keysElem) && keysElem.ValueKind == JsonValueKind.Array)
                    {
                        JsonElement? chosenKey = null;
                        foreach (var k in keysElem.EnumerateArray())
                        {
                            if (k.ValueKind != JsonValueKind.Object) continue;
                            if (TryGetProperty(k, "Clustered", out var cl) && cl.ValueKind == JsonValueKind.True)
                            {
                                chosenKey = k; break;
                            }
                        }
                        if (chosenKey == null)
                        {
                            var first = keysElem.EnumerateArray().FirstOrDefault();
                            if (first.ValueKind == JsonValueKind.Object) chosenKey = first;
                        }
                        if (chosenKey.HasValue && TryGetProperty(chosenKey.Value, "Fields", out var flds) && flds.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var ff in flds.EnumerateArray())
                            {
                                string kname = GetString(ff, "Name");
                                if (!string.IsNullOrWhiteSpace(kname)) tableDef.PrimaryKeys.Add(kname);
                            }
                        }
                    }

                    string key = NormalizeName(tableDef.TableName);
                    if (!tables.ContainsKey(key)) tables[key] = tableDef;
                }
                catch (Exception ex)
                {
                    AppendLog($"[SYMBOL TABLE PARSE ERROR] {ex.Message}");
                }
            }
        }

        private void ParseSymbolFields(JsonElement fieldsArray, TableDefinition tableDef)
        {
            foreach (var f in fieldsArray.EnumerateArray())
            {
                if (f.ValueKind != JsonValueKind.Object) continue;
                string fieldName = GetString(f, "Name");
                if (string.IsNullOrWhiteSpace(fieldName)) continue;

                string sqlType = "text";
                string alType = null;

                if (TryGetProperty(f, "Type", out var ftype) && ftype.ValueKind == JsonValueKind.Object)
                {
                    (sqlType, alType) = MapSymbolTypeToSqlAndAl(ftype);
                }
                else if (TryGetProperty(f, "DataType", out var dtype) && dtype.ValueKind == JsonValueKind.Object)
                {
                    (sqlType, alType) = MapSymbolTypeToSqlAndAl(dtype);
                }

                string tableRel = null;
                if (TryGetProperty(f, "Relation", out var rel) && rel.ValueKind == JsonValueKind.Object)
                {
                    string t1 = GetString(rel, "Table");
                    string t2 = GetString(rel, "TableName");
                    tableRel = !string.IsNullOrWhiteSpace(t1) ? t1 : t2;
                }
                else if (TryGetProperty(f, "TableRelation", out var tr) && tr.ValueKind == JsonValueKind.String)
                {
                    tableRel = tr.GetString();
                }

                var col = new ColumnDefinition
                {
                    ColumnName = fieldName,
                    SQLType = sqlType,
                    ALType = alType,
                    TableRelation = tableRel
                };
                tableDef.Columns.Add(col);

                if (!string.IsNullOrEmpty(col.TableRelation))
                {
                    string refTable = col.TableRelation.Contains(".")
                        ? col.TableRelation.Split('.')[0].Trim().Trim('"')
                        : col.TableRelation;

                    tableDef.ForeignKeys.Add(new ForeignKeyDefinition
                    {
                        ColumnName = col.ColumnName,
                        ReferencedTable = refTable
                    });
                }
            }
        }

        // Typage robuste: si SQLType inconnu/"text" mais ALType présent, re-mappe
        private string ResolveSqlType(ColumnDefinition col)
        {
            if (!string.IsNullOrWhiteSpace(col.SQLType) &&
                !col.SQLType.Equals("text", StringComparison.OrdinalIgnoreCase))
            {
                return col.SQLType;
            }

            if (!string.IsNullOrWhiteSpace(col.ALType))
            {
                var mapped = MapALTypeToSQL(col.ALType);
                if (!string.IsNullOrWhiteSpace(mapped))
                    return mapped;
            }

            return "text";
        }

        // Map des types SymbolReference -> SQL + AL
        private (string sqlType, string alType) MapSymbolTypeToSqlAndAl(JsonElement typeElem)
        {
            string name =
                GetString(typeElem, "Name")
                ?? GetString(typeElem, "Kind")
                ?? GetString(typeElem, "PrimitiveType")
                ?? GetString(typeElem, "TypeName");

            if (string.IsNullOrWhiteSpace(name))
                return ("text", null);

            int? length = GetInt(typeElem, "Length") ?? GetInt(typeElem, "MaxLength") ?? GetInt(typeElem, "Size");
            int? precision = GetInt(typeElem, "Precision") ?? GetInt(typeElem, "PrecisionDigits");
            int? scale = GetInt(typeElem, "Scale") ?? GetInt(typeElem, "ScaleDigits");

            switch (name.Trim().ToLowerInvariant())
            {
                case "code":
                    {
                        int len = length ?? 50;
                        return ($"varchar({len})", $"Code[{len}]");
                    }
                case "text":
                    {
                        int len = length ?? 250;
                        return ($"varchar({len})", $"Text[{len}]");
                    }
                case "integer": return ("int", "Integer");
                case "biginteger": return ("bigint", "BigInteger");
                case "decimal":
                    {
                        int p = precision ?? 38;
                        int s = scale ?? 20;
                        return ($"decimal({p},{s})", $"Decimal[{p},{s}]");
                    }
                case "date": return ("date", "Date");
                case "time": return ("time", "Time");
                case "datetime": return ("datetime", "DateTime");
                case "boolean": return ("boolean", "Boolean");
                case "guid": return ("char(36)", "Guid");
                case "blob": return ("blob", "BLOB");
                case "enum": return ("int", "Enum");
                case "option": return ("int", "Option");
                default:
                    return ("text", name);
            }
        }

        // Helpers JSON
        private static bool TryGetProperty(JsonElement elem, string name, out JsonElement value)
        {
            value = default;
            return elem.ValueKind == JsonValueKind.Object && elem.TryGetProperty(name, out value);
        }

        private static string GetString(JsonElement elem, string name)
        {
            return TryGetProperty(elem, name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;
        }

        private static int? GetInt(JsonElement elem, string name)
        {
            if (TryGetProperty(elem, name, out var v))
            {
                if (v.ValueKind == JsonValueKind.Number && v.TryGetInt32(out var i)) return i;
                if (v.ValueKind == JsonValueKind.String && int.TryParse(v.GetString(), out var j)) return j;
            }
            return null;
        }

        // Mapping AL -> SQL (pour parsing direct des .al)
        private static readonly Dictionary<string, string> AlTypeToSqlType = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "Code[10]", "varchar(10)" }, { "Code[20]", "varchar(20)" }, { "Code[30]", "varchar(30)" },
            { "Code[50]", "varchar(50)" }, { "Code[100]", "varchar(100)" }, { "Code[250]", "varchar(250)" },
            { "Code[500]", "varchar(500)" }, { "Code[1000]", "varchar(1000)" }, { "Code[2048]", "varchar(2048)" },
            { "Code[Max]", "text" },
            { "Text[10]", "varchar(10)" }, { "Text[30]", "varchar(30)" }, { "Text[50]", "varchar(50)" },
            { "Text[100]", "varchar(100)" }, { "Text[250]", "varchar(250)" }, { "Text[500]", "varchar(500)" },
            { "Text[1000]", "varchar(1000)" }, { "Text[2048]", "varchar(2048)" }, { "Text[Max]", "text" },
            { "Integer", "int" }, { "BigInteger", "bigint" },
            { "Decimal", "decimal" }, { "Decimal[16,2]", "decimal(16,2)" }, { "Decimal[20,2]", "decimal(20,2)" }, { "Decimal[38,20]", "decimal(38,20)" },
            { "Date", "date" }, { "Time", "time" }, { "DateTime", "datetime" },
            { "Boolean", "boolean" }, { "Guid", "char(36)" },
            { "BLOB", "blob" }, { "MediaSet", "blob" }, { "Media", "blob" }, { "MediaLink", "blob" }
        };

        private string MapALTypeToSQL(string alType)
        {
            alType = alType.Trim();

            if (AlTypeToSqlType.TryGetValue(alType, out string sqlType))
                return sqlType;

            if (alType.StartsWith("Enum", StringComparison.OrdinalIgnoreCase)) return "int";
            if (alType.Equals("Option", StringComparison.OrdinalIgnoreCase)) return "int";

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

        private string NormalizeName(string input)
        {
            if (string.IsNullOrWhiteSpace(input)) return input;

            input = input.Replace(".", "");
            var words = input.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            if (words.Length == 0) return input;

            string normalized = words[0];
            for (int i = 1; i < words.Length; i++)
                normalized += char.ToUpper(words[i][0]) + words[i].Substring(1);
            return normalized;
        }

        private ParsedObject ParseALObject(string filePath)
        {
            string content;
            try
            {
                content = File.ReadAllText(filePath, Encoding.UTF8);
            }
            catch (Exception ex)
            {
                AppendLog($"[AL READ ERROR] {filePath}: {ex.Message}");
                return null;
            }

            content = StripComments(content);

            // tableextension
            var tableExtMatch = Regex.Match(
                content,
                @"(?is)\btableextension\s+(\d+)\s+(?:""([^""]+)""|([A-Za-z_][A-Za-z0-9_]*))\s+extends\s+(?:""([^""]+)""|([A-Za-z_][A-Za-z0-9_]*))",
                RegexOptions.IgnoreCase);

            if (tableExtMatch.Success)
            {
                string extNumber = tableExtMatch.Groups[1].Value;
                string extName = !string.IsNullOrEmpty(tableExtMatch.Groups[2].Value) ? tableExtMatch.Groups[2].Value : tableExtMatch.Groups[3].Value;
                string baseName = !string.IsNullOrEmpty(tableExtMatch.Groups[4].Value) ? tableExtMatch.Groups[4].Value : tableExtMatch.Groups[5].Value;

                var ext = new TableExtensionDefinition
                {
                    ExtensionNumber = extNumber,
                    ExtensionName = extName,
                    BaseTableName = baseName
                };

                ParseFieldsAndRelations(content, ext.Columns, ext.ForeignKeys);
                return new ParsedObject { Kind = AlObjectKind.TableExtension, TableExtension = ext };
            }

            // table
            var tableHeaderMatch = Regex.Match(
                content,
                @"(?is)\btable\s+(\d+)\s+(?:""([^""]+)""|([A-Za-z_][A-Za-z0-9_]*))",
                RegexOptions.IgnoreCase);

            if (!tableHeaderMatch.Success) return null;

            string tableNumber = tableHeaderMatch.Groups[1].Value;
            string tableName = !string.IsNullOrEmpty(tableHeaderMatch.Groups[2].Value)
                ? tableHeaderMatch.Groups[2].Value
                : tableHeaderMatch.Groups[3].Value;

            var tableDef = new TableDefinition
            {
                TableNumber = tableNumber,
                TableName = tableName
            };

            ParseFieldsAndRelations(content, tableDef.Columns, tableDef.ForeignKeys);
            ParsePrimaryKey(content, tableDef);

            return new ParsedObject { Kind = AlObjectKind.Table, Table = tableDef };
        }

        private void ParseFieldsAndRelations(string content, List<ColumnDefinition> columns, List<ForeignKeyDefinition> foreignKeys)
        {
            string fieldsBlock = ExtractBlock(content, "fields");
            if (fieldsBlock == null) return;

            string innerFields = fieldsBlock.Substring(1, fieldsBlock.Length - 2);
            var fieldChunks = Regex.Split(innerFields, @"(?=field\s*\()", RegexOptions.IgnoreCase);

            foreach (var chunk in fieldChunks)
            {
                if (string.IsNullOrWhiteSpace(chunk)) continue;

                var headerMatch = Regex.Match(
                    chunk,
                    @"field\s*\(\s*\d+\s*;\s*(?:""([^""]+)""|([A-Za-z_][A-Za-z0-9_]*))\s*;\s*([^\)\{]+)\)\s*(\{.*?\})?",
                    RegexOptions.Singleline | RegexOptions.IgnoreCase
                );
                if (!headerMatch.Success) continue;

                string fieldName = !string.IsNullOrEmpty(headerMatch.Groups[1].Value)
                    ? headerMatch.Groups[1].Value.Trim()
                    : headerMatch.Groups[2].Value.Trim();

                string alType = headerMatch.Groups[3].Value.Trim();

                var col = new ColumnDefinition
                {
                    ColumnName = fieldName,
                    ALType = alType,
                    SQLType = MapALTypeToSQL(alType)
                };

                if (headerMatch.Groups[4].Success && !string.IsNullOrWhiteSpace(headerMatch.Groups[4].Value))
                {
                    string block = headerMatch.Groups[4].Value;

                    var tableRelQuoted = Regex.Match(block, @"TableRelation\s*=\s*""([^""]+)""", RegexOptions.IgnoreCase);
                    var tableRelBare = Regex.Match(block, @"TableRelation\s*=\s*([A-Za-z_][A-Za-z0-9_]*)", RegexOptions.IgnoreCase);

                    string tableRel = null;
                    if (tableRelQuoted.Success) tableRel = tableRelQuoted.Groups[1].Value.Trim();
                    else if (tableRelBare.Success) tableRel = tableRelBare.Groups[1].Value.Trim();

                    if (!string.IsNullOrEmpty(tableRel) &&
                        !tableRel.Equals("undefined", StringComparison.OrdinalIgnoreCase) &&
                        !NormalizeName(tableRel).Equals("NoSeries", StringComparison.OrdinalIgnoreCase))
                    {
                        col.TableRelation = tableRel;
                    }
                }

                columns.Add(col);

                if (!string.IsNullOrEmpty(col.TableRelation))
                {
                    string refTable = col.TableRelation.Contains(".")
                        ? col.TableRelation.Split('.')[0].Trim().Trim('"')
                        : col.TableRelation;

                    foreignKeys.Add(new ForeignKeyDefinition
                    {
                        ColumnName = col.ColumnName,
                        ReferencedTable = refTable
                    });
                }
            }
        }

        private void ParsePrimaryKey(string content, TableDefinition tableDef)
        {
            string keysBlock = ExtractBlock(content, "keys");
            if (keysBlock == null) return;

            var pkMatch = Regex.Match(keysBlock, @"key\s*\(\s*[^;]+;\s*([^)]+)\)", RegexOptions.IgnoreCase);
            if (!pkMatch.Success) return;

            var pkFieldMatches = Regex.Matches(pkMatch.Groups[1].Value, @"""([^""]+)""|([A-Za-z_][A-Za-z0-9_]*)");
            foreach (Match m in pkFieldMatches)
            {
                string name = !string.IsNullOrEmpty(m.Groups[1].Value) ? m.Groups[1].Value : m.Groups[2].Value;
                if (!string.IsNullOrEmpty(name))
                    tableDef.PrimaryKeys.Add(name.Trim());
            }
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
                    if (braceCount == 0) return i;
                }
            }
            return -1;
        }

        private string GenerateSQLScript(List<TableDefinition> baseTables, List<TableDefinition> flattened)
        {
            var sb = new StringBuilder();

            if (GenerateBaseTablesToo)
            {
                sb.AppendLine("-- ===== Tables de base (du projet) =====");
                sb.Append(GenerateTablesAndFKs(baseTables, baseTables));
                sb.AppendLine();
            }

            sb.AppendLine("-- ===== Tables aplanies (par tableextension) =====");
            var lookupForFK = new List<TableDefinition>();
            if (GenerateBaseTablesToo) lookupForFK.AddRange(baseTables);
            lookupForFK.AddRange(flattened);

            sb.Append(GenerateTablesAndFKs(flattened, lookupForFK));

            return sb.ToString();
        }

        private string GenerateTablesAndFKs(List<TableDefinition> tablesToCreate, List<TableDefinition> tablesForLookup)
        {
            var sb = new StringBuilder();

            foreach (var table in tablesToCreate)
            {
                string normalizedTableName = NormalizeName(table.TableName);

                if (!table.Columns.Any())
                {
                    sb.AppendLine($"-- Warning: Table `{normalizedTableName}` est vide et n'a pas été générée (base manquante ?).");
                    sb.AppendLine();
                    continue;
                }

                sb.AppendLine($"CREATE TABLE `{normalizedTableName}` (");
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    var col = table.Columns[i];
                    string normalizedColumnName = NormalizeName(col.ColumnName);
                    bool isPK = table.PrimaryKeys.Any(pk => pk.Equals(col.ColumnName, StringComparison.OrdinalIgnoreCase));

                    string columnType = ResolveSqlType(col);
                    if (isPK && columnType.Equals("text", StringComparison.OrdinalIgnoreCase))
                        columnType = "varchar(255)";

                    sb.Append($"    `{normalizedColumnName}` {columnType} {(isPK ? "NOT NULL" : "NULL")}");
                    sb.AppendLine(i < table.Columns.Count - 1 ? "," : "");
                }

                if (table.PrimaryKeys.Any())
                {
                    string pkCols = string.Join(", ", table.PrimaryKeys.Select(c => $"`{NormalizeName(c)}`"));
                    string pkIdentifier = TruncateIdentifier($"PK_{normalizedTableName}");
                    sb.AppendLine($",    CONSTRAINT `{pkIdentifier}` PRIMARY KEY ({pkCols})");
                }

                sb.AppendLine(");");
                sb.AppendLine();
            }

            foreach (var table in tablesToCreate)
            {
                string normalizedTableName = NormalizeName(table.TableName);

                foreach (var fk in table.ForeignKeys)
                {
                    string normalizedFkColumn = NormalizeName(fk.ColumnName);
                    string normalizedReferencedTable = NormalizeName(fk.ReferencedTable);

                    var referencedTableDef = tablesForLookup.FirstOrDefault(t =>
                        NormalizeName(t.TableName).Equals(normalizedReferencedTable, StringComparison.OrdinalIgnoreCase));

                    if (referencedTableDef == null)
                    {
                        sb.AppendLine($"-- Warning: Table référencée `{normalizedReferencedTable}` non trouvée pour FK sur `{normalizedFkColumn}` dans `{normalizedTableName}`.");
                        continue;
                    }

                    string referencedColumn = referencedTableDef.PrimaryKeys.Any()
                        ? referencedTableDef.PrimaryKeys.First()
                        : "No.";
                    string normalizedReferencedColumn = NormalizeName(referencedColumn);

                    string fkIdentifier = TruncateIdentifier($"FK_{normalizedTableName}_{normalizedReferencedTable}_{normalizedFkColumn}");

                    sb.AppendLine(
                        $"ALTER TABLE `{normalizedTableName}` ADD CONSTRAINT `{fkIdentifier}` " +
                        $"FOREIGN KEY (`{normalizedFkColumn}`) REFERENCES `{normalizedReferencedTable}` (`{normalizedReferencedColumn}`);"
                    );
                }

                sb.AppendLine();
            }

            return sb.ToString();
        }

        private string TruncateIdentifier(string identifier)
        {
            return identifier.Length <= 64 ? identifier : identifier.Substring(0, 64);
        }
        #endregion
    }
}