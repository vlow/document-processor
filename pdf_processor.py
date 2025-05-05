import os
import subprocess
import json
import logging
import requests
from datetime import datetime
from pathlib import Path
import time
import re
import shutil # For moving files across different filesystems if needed

# --- Konfiguration ---
BASE_DIR = Path(__file__).parent # Verzeichnis, in dem das Skript liegt
INBOX_DIR = BASE_DIR / "Inbox"
PROCESSED_DIR = BASE_DIR / "Processed"
FAILED_DIR = BASE_DIR / "Failed"
LOG_FILE = BASE_DIR / "pdf_processing.log"
OCRMYPDF_PATH = "ocrmypdf" # Oder der volle Pfad, falls nicht im PATH
GHOSTSCRIPT_PATH = "gs"     # Oder der volle Pfad zu Ghostscript
OLLAMA_URL = "http://localhost:11434/api/generate" # Standard Ollama API Endpunkt
LLM_MODEL = "mistral-small3.1:latest" # Passe dies an dein Llama 3 Modell an (z.B. llama3:8b)
LLM_TIMEOUT = 60 # Sekunden Timeout für die LLM-Antwort
MAX_TEXT_LENGTH = 4000 # Anzahl Zeichen auf die der Textinput für das LLM gekürzt wird
DOCUMENT_CATEGORIES = ["Ausbildung", "Bank", "Steuer", "Rechnung", "Versicherung", "Gesundheit", "Vertrag", "Gehalt", "Behörde", "Sonstiges"]

# --- Logging einrichten ---
def setup_logging():
    """Konfiguriert das Logging für Datei und Konsole."""
    # Verhindere doppeltes Hinzufügen von Handlern, falls Funktion erneut aufgerufen wird
    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()

    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger.setLevel(logging.INFO)

    # File Handler
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)

# --- Hilfsfunktionen ---
def sanitize_filename(name):
    """Entfernt ungültige Zeichen aus Dateinamen."""
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip(' _')
    max_len = 200
    if len(name) > max_len:
        parts = name[:max_len].split(' ')
        if len(parts) > 1:
            name = ' '.join(parts[:-1])
        else:
            name = name[:max_len]
    return name

def run_ghostscript_repair(input_path: Path, output_path: Path) -> bool:
    """Versucht, eine PDF mit Ghostscript zu reparieren."""
    command = [
        GHOSTSCRIPT_PATH,
        "-o", str(output_path),
        "-sDEVICE=pdfwrite",
        "-dPDFSETTINGS=/prepress", # Eine gängige Einstellung zur Neuinterpretation/Reparatur
        str(input_path)
    ]
    logging.info(f"Versuche Ghostscript-Reparatur für '{input_path.name}' -> '{output_path.name}'...")
    print(f"    -> Versuche PDF-Reparatur mit Ghostscript...")
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8')
        logging.info(f"Ghostscript-Reparatur erfolgreich für '{input_path.name}'.")
        # logging.debug(f"Ghostscript stdout:\n{result.stdout}")
        # logging.debug(f"Ghostscript stderr:\n{result.stderr}")
        print(f"    -> PDF-Reparatur erfolgreich.")
        return True
    except FileNotFoundError:
        logging.error(f"Fehler: '{GHOSTSCRIPT_PATH}' nicht gefunden. Ist Ghostscript installiert und im PATH?")
        print(f"    -> FEHLER: Ghostscript nicht gefunden!")
        return False
    except subprocess.CalledProcessError as e:
        logging.error(f"Fehler bei der Ghostscript-Reparatur von '{input_path.name}': {e}")
        logging.error(f"Exit-Code: {e.returncode}")
        stderr_decoded = e.stderr.decode('utf-8', errors='ignore') if isinstance(e.stderr, bytes) else e.stderr
        logging.error(f"Ghostscript stderr:\n{stderr_decoded}")
        print(f"    -> FEHLER bei der PDF-Reparatur.")
        return False
    except Exception as e:
         logging.error(f"Unerwarteter Fehler bei der Ghostscript-Reparatur von '{input_path.name}': {e}")
         print(f"    -> FEHLER bei der PDF-Reparatur.")
         return False

def run_ocrmypdf(input_path: Path, output_path: Path, is_retry=False) -> bool:
    """Führt ocrmypdf aus, versucht Reparatur bei Ghostscript-Fehler (Exit Code 7)."""
    ocrmypdf_command = [
        OCRMYPDF_PATH,
        "--output-type", "pdfa",
        "--force-ocr",
        "--language", "deu+eng",
        "--rotate-pages",
        "--deskew",
        str(input_path),
        str(output_path)
    ]

    action = "Versuche OCR erneut" if is_retry else "Führe OCR"
    logging.info(f"{action} für '{input_path.name}' aus...")
    try:
        result = subprocess.run(ocrmypdf_command, capture_output=True, text=True, check=True, encoding='utf-8')
        logging.info(f"OCR erfolgreich für '{input_path.name}'. Output in '{output_path.name}'.")
        return True
    except FileNotFoundError:
        logging.error(f"Fehler: '{OCRMYPDF_PATH}' nicht gefunden. Ist ocrmypdf installiert und im PATH?")
        return False
    except subprocess.CalledProcessError as e:
        stderr_decoded = e.stderr.decode('utf-8', errors='ignore') if isinstance(e.stderr, bytes) else e.stderr
        logging.error(f"Fehler bei der OCR-Verarbeitung von '{input_path.name}': {e}")
        logging.error(f"Exit-Code: {e.returncode}")
        logging.error(f"ocrmypdf stderr:\n{stderr_decoded}")

        # --- NEU: Ghostscript Reparaturversuch ---
        # Prüfe auf spezifischen Fehler (Exit Code 7 und Ghostscript im stderr) und ob es der erste Versuch ist
        if e.returncode == 7 and "ghostscript" in stderr_decoded.lower() and not is_retry:
            logging.warning(f"Ghostscript-Problem bei OCR für '{input_path.name}' erkannt (Exit Code 7). Versuche Reparatur.")
            print(f"  -> Ghostscript-Problem bei OCR erkannt. Versuche Reparatur...")

            # Baue den Dateinamen korrekt zusammen: alter_name + _repaired_temp + .pdf
            repaired_filename = f"{input_path.stem}_repaired_temp{input_path.suffix}"
            repaired_temp_path = input_path.parent / repaired_filename

            # Stelle sicher, dass die reparierte temporäre Datei nicht schon existiert
            repaired_temp_path.unlink(missing_ok=True)

            # Versuche Reparatur mit Ghostscript (originale Datei -> reparierte Temp-Datei)
            repair_successful = run_ghostscript_repair(input_path, repaired_temp_path)

            if repair_successful:
                # Versuche OCR erneut mit der reparierten Datei
                logging.info(f"Wiederhole OCR-Versuch mit reparierter Datei '{repaired_temp_path.name}' -> '{output_path.name}'.")
                ocr_retry_successful = run_ocrmypdf(repaired_temp_path, output_path, is_retry=True)

                # Lösche die temporäre reparierte Datei
                logging.info(f"Lösche temporäre reparierte Datei '{repaired_temp_path.name}'.")
                repaired_temp_path.unlink(missing_ok=True)

                return ocr_retry_successful # Gib das Ergebnis des zweiten Versuchs zurück
            else:
                logging.error(f"Ghostscript-Reparatur für '{input_path.name}' fehlgeschlagen. OCR wird abgebrochen.")
                # Lösche ggf. eine leere oder fehlerhafte reparierte Datei
                repaired_temp_path.unlink(missing_ok=True)
                return False # Reparatur fehlgeschlagen, daher OCR fehlgeschlagen
        else:
            # Anderer ocrmypdf-Fehler oder zweiter Versuch ist fehlgeschlagen
            logging.error(f"OCR endgültig fehlgeschlagen für '{input_path.name}'.")
            return False # Standardfehler bei ocrmypdf
        # --- Ende NEU ---

    except Exception as e:
         logging.error(f"Unerwarteter Fehler bei der OCR-Verarbeitung von '{input_path.name}': {e}")
         return False


def extract_text_from_pdf(pdf_path: Path) -> str | None:
    """Extrahiert Text aus einem PDF."""
    logging.info(f"Extrahiere Text aus '{pdf_path.name}'...")
    try:
        from pypdf import PdfReader # Import erst hier, um Fehler anzuzeigen, falls nicht installiert
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            extracted = page.extract_text()
            if extracted: # Nur hinzufügen, wenn Text extrahiert wurde
                 text += extracted + "\n" # Füge Zeilenumbruch zwischen Seiten hinzu
        if not text.strip():
             logging.warning(f"Kein Textinhalt extrahiert aus '{pdf_path.name}'. Ist das Dokument leer oder rein bildbasiert trotz OCR?")
             # Hier könnte man überlegen, ob man es als Fehler wertet
        logging.info(f"Textextraktion für '{pdf_path.name}' abgeschlossen.")
        return text
    except ImportError:
        logging.error("Fehler: PyPDF-Bibliothek nicht gefunden. Installiere sie mit 'pip install pypdf'.")
        return None
    except Exception as e:
        logging.error(f"Fehler bei der Textextraktion aus '{pdf_path.name}': {e}")
        return None

def analyze_text_with_llm(text: str) -> dict | None:
    """Sendet Text an Ollama und extrahiert strukturierte Informationen."""
    prompt = f"""
Analysiere den folgenden Text aus einem gescannten Dokument (z.B. Brief, Rechnung, Bescheid).
Extrahiere die folgenden Informationen und gib sie ausschließlich als JSON-Objekt zurück:
1.  "datum": Das Hauptdatum des Dokuments (z.B. Ausstellungsdatum, Rechnungsdatum). Bevorzuge das erstgenannte oder prominenteste Datum. Gib es immer im Format JJJJ-MM-DD zurück. Wenn du ein deutsches Datum wie TT.MM.JJJJ oder TT MM JJJJ findest, konvertiere es. Wenn kein Datum gefunden wird, gib "null" zurück.
2.  "absender": Der Name der Institution oder Firma, die das Dokument gesendet hat (z.B. "Finanzamt München", "BARMER Ersatzkasse", "Stadtwerke Beispielstadt"). Gib *nur* den Namen der Institution an, auch wenn eine Person im Namen der Institution unterschrieben hat. Wenn kein Absender klar ersichtlich ist, gib "Unbekannt" zurück.
3.  "titel": Ein kurzer, prägnanter Titel für das Dokument. Nutze wenn möglich den Betreff ("Betreff:", "Subject:"). Wenn kein Betreff vorhanden ist, fasse den Hauptzweck in 3-6 Worten zusammen (z.B. "Steuerbescheid Einkommensteuer 2023", "Rechnung Strom April 2024", "Kontoauszug Mai 2024").
4.  "kategorie": Ordne das Dokument einer der folgenden Kategorien zu: {", ".join(DOCUMENT_CATEGORIES)}. Wähle ausschließlich aus diesen Kategorien die passendste basierend auf Absender und Inhalt.

Hier ist der extrahierte Text:
--- TEXT START ---
{text[:MAX_TEXT_LENGTH]}
--- TEXT END ---

Gib NUR das JSON-Objekt zurück, ohne einleitenden oder abschließenden Text. Beispiel:
{{
  "datum": "2024-05-15",
  "absender": "Beispiel GmbH",
  "titel": "Rechnung Nr. 12345",
  "kategorie": "Rechnung"
}}
"""
    # Begrenze die Textlänge, um das Kontextfenster des LLM nicht zu sprengen
    if len(text) > MAX_TEXT_LENGTH:
         logging.warning(f"Text für LLM Analyse gekürzt auf {MAX_TEXT_LENGTH} Zeichen.")

    logging.info(f"Sende Text zur Analyse an LLM '{LLM_MODEL}'...")
    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": LLM_MODEL,
                "prompt": prompt,
                "stream": False, # Wir wollen die gesamte Antwort auf einmal
                "format": "json" # Fordere explizit JSON-Format an (neuere Ollama-Versionen)
            },
            timeout=LLM_TIMEOUT
        )
        response.raise_for_status() # Wirft Fehler bei HTTP-Statuscodes >= 400

        try:
            api_response_json = response.json()
            llm_output_str = api_response_json.get("response", "")
            result_json = json.loads(llm_output_str)
            logging.info(f"LLM Analyse erfolgreich.")
            required_keys = ["datum", "absender", "titel", "kategorie"]
            if not all(key in result_json for key in required_keys):
                 logging.error(f"LLM Antwort enthält nicht alle erwarteten Schlüssel: {result_json}")
                 return None
            return result_json

        except json.JSONDecodeError:
             # Logge den Parsing Fehler und die empfangene Antwort
             llm_output_str_log = api_response_json.get("response", response.text) # Nimm response.text als Fallback
             logging.error(f"Fehler beim Parsen der LLM JSON-Antwort. Empfangener String: '{llm_output_str_log}'")
             match = re.search(r'\{.*\}', llm_output_str_log, re.DOTALL)
             if match:
                 try:
                     extracted_json_str = match.group(0)
                     result_json = json.loads(extracted_json_str)
                     logging.info("JSON erfolgreich aus LLM-Antwort extrahiert nach Parsing-Fehler.")
                     required_keys = ["datum", "absender", "titel", "kategorie"]
                     if not all(key in result_json for key in required_keys):
                          logging.error(f"Extrahiertes JSON enthält nicht alle erwarteten Schlüssel: {result_json}")
                          return None
                     return result_json
                 except json.JSONDecodeError:
                     logging.error(f"Konnte auch nach Extraktion kein valides JSON aus der LLM-Antwort parsen. Extrahierter String: '{extracted_json_str}'")
                     return None
             else:
                logging.error("Kein JSON-ähnlicher String in der LLM-Antwort gefunden.")
                return None
        except Exception as e:
             logging.error(f"Unerwarteter Fehler beim Verarbeiten der LLM-Antwort: {e}")
             return None

    except requests.exceptions.ConnectionError:
        logging.error(f"Fehler: Verbindung zu Ollama unter '{OLLAMA_URL}' fehlgeschlagen. Läuft der Ollama Server?")
        return None
    except requests.exceptions.Timeout:
         logging.error(f"Fehler: Timeout bei der Anfrage an Ollama nach {LLM_TIMEOUT} Sekunden.")
         return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Fehler bei der Anfrage an Ollama: {e}")
        return None
    except Exception as e:
         logging.error(f"Unerwarteter Fehler bei der LLM-Analyse: {e}")
         return None


def get_next_available_filename(target_path: Path) -> Path:
    """Findet einen freien Dateinamen, falls die Zieldatei existiert (fügt Suffix hinzu)."""
    if not target_path.exists():
        return target_path

    base = target_path.stem
    ext = target_path.suffix
    parent = target_path.parent
    counter = 1
    while True:
        new_name = f"{base} ({counter}){ext}"
        new_path = parent / new_name
        if not new_path.exists():
            return new_path
        counter += 1

# --- Hauptlogik ---
def main():
    """Hauptfunktion zur Verarbeitung der PDFs."""
    setup_logging()
    logging.info("Starte PDF Verarbeitungsskript.")
    print("Starte PDF Verarbeitung...")

    # Stelle sicher, dass die Zielordner existieren
    INBOX_DIR.mkdir(exist_ok=True)
    PROCESSED_DIR.mkdir(exist_ok=True)
    FAILED_DIR.mkdir(exist_ok=True)

    pdf_files = list(INBOX_DIR.glob("*.pdf"))
    # Ignoriere temporäre Dateien im Inbox-Ordner
    pdf_files = [f for f in pdf_files if not f.name.endswith(('_ocr_temp.pdf', '_repaired_temp.pdf'))]

    total_files = len(pdf_files)
    processed_count = 0
    failed_files = []
    success_log = []

    if not pdf_files:
        logging.info("Keine verarbeitbaren PDF-Dateien im Inbox-Ordner gefunden.")
        print("Keine verarbeitbaren PDF-Dateien im Inbox-Ordner gefunden.")
        return

    logging.info(f"Gefundene PDF-Dateien im Inbox-Ordner: {total_files}")
    print(f"Gefundene PDF-Dateien im Inbox-Ordner: {total_files}")

    for i, pdf_path in enumerate(pdf_files):
        print(f"\n[{i+1}/{total_files}] Verarbeite Datei: {pdf_path.name}")
        logging.info(f"--- Starte Verarbeitung für: {pdf_path.name} ---")
        original_name = pdf_path.name
        # Definiere den Pfad für die (potenzielle) OCR-Ausgabedatei
        temp_ocr_pdf_path = INBOX_DIR / f"{pdf_path.stem}_ocr_temp.pdf"

        # Lösche alte temporäre OCR-Datei, falls vorhanden (von einem früheren Lauf)
        temp_ocr_pdf_path.unlink(missing_ok=True)

        try:
            # 1. OCR durchführen (mit integriertem Reparaturversuch)
            print("  Schritt 1: Führe OCR durch...")
            # WICHTIG: Die run_ocrmypdf Funktion nimmt die *originale* PDF als Input,
            # kümmert sich intern um die Reparatur und den erneuten Versuch,
            # und schreibt das *finale* OCR-Ergebnis nach temp_ocr_pdf_path.
            if not run_ocrmypdf(pdf_path, temp_ocr_pdf_path):
                raise Exception(f"OCR fehlgeschlagen (auch nach möglichem Reparaturversuch) für {pdf_path.name}")

            # Stelle sicher, dass die OCR-Datei auch wirklich erstellt wurde
            if not temp_ocr_pdf_path.exists():
                 raise Exception(f"OCR-Ausgabedatei {temp_ocr_pdf_path.name} wurde nicht erstellt, obwohl run_ocrmypdf True zurückgab.")

            # 2. Text extrahieren (aus der finalen OCR-Datei)
            print("  Schritt 2: Extrahiere Text...")
            text = extract_text_from_pdf(temp_ocr_pdf_path)
            if not text:
                # Wenn kein Text extrahiert wurde, obwohl OCR erfolgreich war, ist das Dokument evtl. leer/problematisch
                logging.warning(f"Textextraktion ergab keinen Inhalt für {temp_ocr_pdf_path.name}, obwohl OCR erfolgreich war.")
                # Entscheide, ob dies als Fehler behandelt werden soll
                raise Exception(f"Textextraktion ergab keinen Inhalt für {temp_ocr_pdf_path.name}")
            if len(text.strip()) < 50: # Prüfe auf minimal sinnvollen Text
                 logging.warning(f"Sehr kurzer Text (<50 Zeichen) extrahiert aus {temp_ocr_pdf_path.name}.")
                 # raise Exception(f"Extrahierter Text zu kurz für {temp_ocr_pdf_path.name}") # Optional

            # 3. LLM Analyse
            print("  Schritt 3: Analysiere Text mit LLM...")
            analysis_result = analyze_text_with_llm(text)
            if not analysis_result:
                raise Exception(f"LLM Analyse fehlgeschlagen für {temp_ocr_pdf_path.name}")

            # 4. Informationen validieren und Dateinamen erstellen
            print("  Schritt 4: Validiere Daten und erstelle Dateinamen...")
            datum_str = analysis_result.get("datum")
            absender = analysis_result.get("absender", "Unbekannt").strip()
            titel = analysis_result.get("titel", "Unbenannt").strip()
            kategorie = analysis_result.get("kategorie", "Sonstiges").strip().capitalize()

            # Datum validieren
            if datum_str:
                try:
                    datetime.strptime(datum_str, '%Y-%m-%d')
                except (ValueError, TypeError): # TypeError fängt None ab
                    logging.warning(f"LLM gab ungültiges oder fehlendes Datum zurück: '{datum_str}'. Verwende 'NODATE'.")
                    datum_str = "NODATE"
            else:
                logging.warning(f"Kein Datum vom LLM extrahiert für {original_name}. Verwende 'NODATE'.")
                datum_str = "NODATE"

            absender_sanitized = sanitize_filename(absender)
            titel_sanitized = sanitize_filename(titel)
            kategorie_sanitized = sanitize_filename(kategorie).replace(" ", "_")

            if not absender_sanitized: absender_sanitized = "Unbekannter_Absender"
            if not titel_sanitized: titel_sanitized = "Unbenannter_Titel"
            if not kategorie_sanitized: kategorie_sanitized = "Sonstiges"

            new_filename = f"{datum_str} - {absender_sanitized} - {titel_sanitized}.pdf"

            # 5. Zielordner erstellen
            target_category_dir = PROCESSED_DIR / kategorie_sanitized
            target_category_dir.mkdir(parents=True, exist_ok=True)

            # 6. Datei umbenennen und verschieben (die OCR'd Temp-Datei)
            target_path = target_category_dir / new_filename
            final_target_path = get_next_available_filename(target_path)
            if final_target_path != target_path:
                logging.warning(f"Zieldatei '{target_path.name}' existiert bereits. Speichere als '{final_target_path.name}'.")
                print(f"  Info: Zieldatei existiert, speichere als '{final_target_path.name}'.")

            print(f"  Schritt 5: Verschiebe Datei nach '{final_target_path}'...")
            shutil.move(str(temp_ocr_pdf_path), str(final_target_path))

            # 7. Ursprüngliche Datei löschen
            pdf_path.unlink()
            logging.info(f"Originaldatei '{pdf_path.name}' gelöscht.")


            logging.info(f"Erfolgreich verarbeitet: '{original_name}' -> '{final_target_path}'")
            success_log.append({
                "original": original_name,
                "neu": final_target_path.name,
                "ziel": str(final_target_path.relative_to(BASE_DIR))
            })
            processed_count += 1

        except Exception as e:
            logging.error(f"FEHLER bei der Verarbeitung von '{original_name}': {e}", exc_info=False)
            print(f"  FEHLER bei der Verarbeitung von '{original_name}': {e}")
            failed_files.append(original_name)
            # Versuche, die temporäre OCR-Datei zu löschen, falls sie existiert und der Fehler *danach* auftrat
            temp_ocr_pdf_path.unlink(missing_ok=True)
            # Die temporäre reparierte Datei wird bereits innerhalb von run_ocrmypdf gelöscht

        finally:
            # Kurze Pause
            time.sleep(0.5)

    # --- Abschluss ---
    print("\n--- Verarbeitung abgeschlossen ---")
    logging.info("--- Verarbeitung abgeschlossen ---")

    # Verschiebe fehlgeschlagene Dateien
    if failed_files:
        print(f"\nVerschiebe {len(failed_files)} fehlgeschlagene Dateien nach '{FAILED_DIR.name}'...")
        logging.info(f"Verschiebe {len(failed_files)} fehlgeschlagene Dateien nach '{FAILED_DIR.name}'...")
        for filename in failed_files:
            source_path = INBOX_DIR / filename
            target_path = FAILED_DIR / filename
            if source_path.exists():
                try:
                    # Stelle sicher, dass die Zieldatei nicht bereits existiert (kann passieren, wenn das Skript mehrmals läuft)
                    if target_path.exists():
                         # Füge einen Timestamp hinzu oder überschreibe - hier wird überschrieben
                         logging.warning(f"Fehlgeschlagene Datei '{target_path.name}' existiert bereits im Failed-Ordner. Überschreibe.")
                         target_path.unlink()
                    shutil.move(str(source_path), str(target_path))
                    logging.info(f"Fehlgeschlagene Datei '{filename}' nach '{FAILED_DIR.name}' verschoben.")
                except Exception as move_err:
                     logging.error(f"Konnte fehlgeschlagene Datei '{filename}' nicht nach '{FAILED_DIR.name}' verschieben: {move_err}")
                     print(f"  FEHLER: Konnte '{filename}' nicht nach '{FAILED_DIR.name}' verschieben: {move_err}")
            else:
                 logging.warning(f"Ursprüngliche Datei '{filename}' für fehlgeschlagenen Move nicht mehr in Inbox gefunden.")


    # Zusammenfassung ausgeben
    print("\n--- Zusammenfassung ---")
    print(f"Gesamtzahl der Dateien: {total_files}")
    print(f"Erfolgreich verarbeitet: {processed_count}")
    print(f"Fehlgeschlagen: {len(failed_files)}")

    if failed_files:
        print("\nFehlgeschlagene Dateien (verschoben nach 'Failed'):")
        for f in failed_files:
            print(f"- {f}")
        logging.warning(f"Fehlgeschlagene Dateien: {', '.join(failed_files)}")

    logging.info(f"Zusammenfassung: Gesamt={total_files}, Erfolgreich={processed_count}, Fehlgeschlagen={len(failed_files)}")
    print(f"\nLogdatei wurde nach '{LOG_FILE}' geschrieben.")

if __name__ == "__main__":
    main()
