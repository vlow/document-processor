# PDF Prozessor & Organizer

Ein Python-Tool zur automatischen Verarbeitung, Kategorisierung, Umbenennung und Organisation gescannter PDF-Dokumente aus einem 'Inbox'-Ordner mittels OCR und einem lokalen Large Language Model (LLM) via Ollama.

Dieses Skript soll die mühsame Aufgabe der digitalen Dokumentenablage automatisieren, indem es automatisch Schlüsselinformationen extrahiert und die Dateien in eine sinnvolle Ordnerstruktur verschiebt.

## Funktionen

* **Batch Verarbeitung eines `Inbox`-Ordners:** Scannt automatisch einen festgelegten Ordner auf neue PDF-Dateien.
* **OCR-Verarbeitung:** Nutzt `ocrmypdf` zur Durchführung von Optical Character Recognition (OCR) bei gescannten PDFs, um diese durchsuchbar zu machen. Beinhaltet Optionen zur Korrektur von Schräglagen (Deskew) und Seitenausrichtung.
* **PDF-Reparatur:** Versucht automatisch, potenziell beschädigte PDFs mittels Ghostscript zu reparieren, falls `ocrmypdf` auf spezifische Fehler bei der Rasterisierung stößt (tritt oft bei PDFs mit ungewöhnlicher Formatierung auf).
* **LLM-Analyse:** Extrahiert Text aus der OCR-verarbeiteten PDF und sendet ihn zur Analyse an ein lokal laufendes LLM (via Ollama).
* **Informationsextraktion:** Das LLM extrahiert folgende Details:
    * **Datum:** Das Hauptdatum des Dokuments (Ausstellungsdatum, Rechnungsdatum etc.), formatiert als `JJJJ-MM-TT`.
    * **Absender:** Der Name der absendenden Institution oder Firma (z. B. „Finanzamt München“, „BARMER Ersatzkasse“). Priorisiert die Institution gegenüber Personennamen.
    * **Titel:** Ein prägnanter Titel, vorzugsweise aus der Betreffzeile des Dokuments oder eine kurze Zusammenfassung (z. B. „Steuerbescheid Einkommensteuer 2023“, „Rechnung Strom April 2025“).
    * **Kategorie:** Weist eine passende Kategorie zu (z. B. `Bank`, `Steuer`, `Rechnung`, `Gesundheit`, `Versicherung`, `Vertrag`, `Gehalt`, `Behörde`, `Sonstiges`).
* **Konsistente Umbenennung:** Benennt Dateien anhand der extrahierten Informationen im Format: `JJJJ-MM-TT - Absender - Titel.pdf`. Ungültige Zeichen für Dateinamen werden automatisch bereinigt.
* **Strukturierte Organisation:** Verschiebt die umbenannten Dateien in eine kategorisierte Ordnerstruktur: `Processed/KategorieName/`.
* **Fehlerbehandlung:** Überspringt Dateien, die während der Verarbeitung Fehler verursachen, und verschiebt sie zur manuellen Überprüfung in einen `Failed`-Ordner.
* **Logging:** Protokolliert alle Aktionen, Erfolge und Fehler in einer Logdatei (`pdf_processing.log`) und gibt Fortschritts-Updates auf der Konsole aus.

## Voraussetzungen

* **Python 3.x**
* **ocrmypdf:** Für die OCR-Verarbeitung.
* **Ghostscript (`gs`):** Für den PDF-Reparaturschritt.
* **Ollama:** Muss lokal laufen und das LLM bereitstellen. ([https://ollama.com/](https://ollama.com/))
* **Ein Ollama LLM-Modell:** Ein Modell muss via Ollama heruntergeladen worden sein (siehe Konfiguration).
* **Python-Bibliotheken:** `requests`, `pypdf`

## Installation

1.  **Repository klonen:**
    ```bash
    git clone https://github.com/vlow/pdf-processor
    cd pdf-processor
    ```

2.  **Python-Bibliotheken installieren:**
    ```bash
    pip install requests pypdf
    # oder installation über den jeweiligen Paketmanager der Distribution.
    ```

3.  **`ocrmypdf` installieren:**
    * **Linux (Debian/Ubuntu):**
        ```bash
        sudo apt update && sudo apt install ocrmypdf
        ```
    * **macOS (mittels Homebrew):**
        ```bash
        brew install ocrmypdf
        ```
    * Für andere Systeme oder Details, siehe die [offizielle ocrmypdf-Dokumentation](https://ocrmypdf.readthedocs.io/en/latest/installation.html).

4.  **Ghostscript installieren:**
    * **Linux (Debian/Ubuntu):** Oft als Abhängigkeit von `ocrmypdf` mitinstalliert, aber zur Sicherheit:
        ```bash
        sudo apt update && sudo apt install ghostscript
        ```
    * **macOS (mittels Homebrew):** Oft als Abhängigkeit von `ocrmypdf` mitinstalliert, aber zur Sicherheit:
        ```bash
        brew install ghostscript
        ```

5.  **Ollama installieren und starten:**
    * Ollama von [ollama.com](https://ollama.com/) herunterladen und installieren.
    * Die Ollama-Anwendung/den Dienst starten.
    * Das gewünschte LLM-Modell herunterladen (siehe Konfiguration unten). Zum Beispiel:
        ```bash
        ollama pull llama3:latest # Oder ein anderes Modell
        # Empfohlen für deutsche Dokumente (dieses herunterladen):
        ollama pull mistral-small3.1:latest
        ```
    * Überprüfen, ob das Modell verfügbar ist: `ollama list`

## Konfiguration

Konfiguriere das Skript durch Bearbeiten der Variablen am Anfang der Datei `pdf_processor.py`:

```python
# --- Konfiguration ---
BASE_DIR = Path(__file__).parent # Verzeichnis, in dem das Skript liegt
INBOX_DIR = BASE_DIR / "Inbox"  # Ordner, der auf eingehende PDFs gescannt wird
PROCESSED_DIR = BASE_DIR / "Processed" # Hauptordner für organisierte PDFs
FAILED_DIR = BASE_DIR / "Failed"    # Ordner für fehlgeschlagene PDFs
LOG_FILE = BASE_DIR / "pdf_processing.log" # Pfad zur Logdatei
OCRMYPDF_PATH = "ocrmypdf"      # Befehl/Pfad für ocrmypdf
GHOSTSCRIPT_PATH = "gs"         # Befehl/Pfad für Ghostscript
OLLAMA_URL = "http://localhost:11434/api/generate" # Ollama API Endpunkt
LLM_MODEL = "mistral-small3.1:latest" # Ollama-Modell für die Analyse *<-- WICHTIG*
LLM_TIMEOUT = 120               # Timeout in Sekunden für die LLM-Antwort
MAX_TEXT_LENGTH = 4000 # Anzahl Zeichen auf die der Textinput für das LLM gekürzt wird
DOCUMENT_CATEGORIES = ["Ausbildung", "Bank", "Steuer", "Rechnung", "Versicherung", "Gesundheit", "Vertrag", "Gehalt", "Behörde", "Sonstiges"] # Kategorien für Unterordner
```

* **`*_DIR` / `LOG_FILE`:** Definiert die Arbeitsverzeichnisse und den Speicherort der Logdatei. Standardmäßig relativ zum Speicherort des Skripts.
* **`OCRMYPDF_PATH`, `GHOSTSCRIPT_PATH`:** Normalerweise als Standardwerte in Ordnung, wenn die Tools im Systempfad (`PATH`) verfügbar sind. Ändern, falls sie woanders installiert sind.
* **`OLLAMA_URL`:** Ändern, falls deine Ollama-Instanz unter einer anderen Adresse oder einem anderen Port läuft.
* **`LLM_MODEL`:** **Entscheidend!** Setze dies auf den exakten Namen des Modells, das du in Ollama heruntergeladen hast und verwenden möchtest (z. B. `llama3:latest`, `mistral:7b`).
    * **Empfehlung für deutsche Dokumente:** Basierend auf Tests lieferte `mistral-small3.1:latest` gute Ergebnisse für deutsche Dokumente. Stelle sicher, dass du dieses Modell heruntergeladen hast (`ollama pull mistral-small3.1:latest`) und setze `LLM_MODEL = "mistral-small3.1:latest"`.
* **`LLM_TIMEOUT`:** Anpassen, falls dein LLM länger für eine Antwort benötigt (z. B. bei größeren Modellen oder langsamerer Hardware).
* **`MAX_TEXT_LENGTH`:** Lange Dokumente verlangsamen den LLM Prozess und verschlechtern das Ergebnis. Dieser Parameter beschränkt die maximale Textlänge, die an das LLM gegeben wird. Da sich alle relevanten Informationen (Absender, Datum, Betreff etc.) in der Regel auf der ersten Dokumentseite befinden, reichen 4000 Zeichen für unsere Zwecke locker aus.
* **`DOCUMENT_CATEGORIES`:** Die Kategorien, in die das LLM die Dokumente einordnen soll.

## Benutzung

1.  **PDFs platzieren:** Verschiebe oder kopiere die zu verarbeitenden gescannten PDF-Dateien in den `Inbox`-Ordner.
2.  **Sicherstellen, dass Ollama läuft:** Stelle sicher, dass die Ollama-Anwendung/der Dienst aktiv ist und das in `LLM_MODEL` angegebene Modell verfügbar ist (`ollama list`).
3.  **Skript ausführen:** Öffne ein Terminal, navigiere in das Verzeichnis, das `pdf_processor.py` enthält, und führe aus:
    ```bash
    python pdf_processor.py
    ```
4.  **Beobachten:** Verfolge die Konsolenausgabe für Fortschritt und mögliche Fehler.
5.  **Ergebnisse prüfen:** Überprüfe nach Abschluss den `Processed`-Ordner auf erfolgreich organisierte Dateien und den `Failed`-Ordner auf Dateien, die nicht verarbeitet werden konnten.
6.  **Log prüfen:** Untersuche die Datei `pdf_processing.log` für eine detaillierte Historie der Operationen und Fehler.

## Ordnerstruktur

Das Skript verwendet die folgende Ordnerstruktur in seinem Verzeichnis:

* `Inbox/`: Platziere hier deine eingehenden gescannten PDFs. Originale werden nach erfolgreicher Verarbeitung von hier gelöscht.
* `Processed/`: Erfolgreich verarbeitete Dateien werden hierhin verschoben.
    * `KategorieName/`: Unterordner werden automatisch basierend auf der vom LLM ermittelten Kategorie erstellt (z. B. `Processed/Steuer/`, `Processed/Rechnung/`).
* `Failed/`: Original-PDFs, bei denen während der Verarbeitung ein nicht behebbarer Fehler auftrat, werden zur manuellen Inspektion hierhin verschoben.
* `pdf_processor.py`: Das Hauptskript.
* `pdf_processing.log`: Die Logdatei.

## Lizenz

Dieses Projekt ist unter der MIT-Lizenz lizenziert.
