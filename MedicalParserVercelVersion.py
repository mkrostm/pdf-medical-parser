#!/usr/bin/env python3
"""
PDF Medical Claims Parser

A comprehensive tool for extracting patient information, claims data, and 
service details from medical PDF documents.

Author: Converted to class-based structure
Date: 2025
"""

import sys
import subprocess
import importlib


import re
import os
import time
import multiprocessing as mp
from pathlib import Path
from flask import Flask, request, jsonify, send_file
from werkzeug.utils import secure_filename
import io
import tempfile
import fitz
import pandas
class MedicalPDFClaimsParser:
    """
    A class for parsing medical PDF documents to extract patient claims information.
    
    This parser extracts patient details, provider information, claim numbers,
    service codes, dates of service, and other relevant medical billing data.
    """
    
    # Class constants for regex patterns
    PATIENT_TAG_PATTERN = r"\nPATIENT\s*:"   
    PATIENT_RESPONSE_TAG_PATTERN = r'\n\s*PAT\s*RESP\s*:'
    
    # Field extraction patterns
    FIELD_EXTRACTION_PATTERNS = {
        "Patient Name": re.compile(r"PATIENT\s*:(.+?)\s*PATIENT", re.DOTALL),
        "Patient ID": re.compile(r"PATIENT\s*ID\s*\#:(.+?)\s*CONTRACT\s*", re.DOTALL),
        "Provider Name": re.compile(r'REND\s*PROV\s*:(.+?)REND', re.DOTALL),
        "Provider ID": re.compile(r'PROV\s*ID\s*:(.+?)PROV', re.DOTALL),
        "Patient CTRL": re.compile(r"PAT\s*CTRL\s*\#\s*:(.+?)CLM", re.DOTALL),
        "Provider CTRL": re.compile(r'PROV\s*CTRL\s*NBR\s*:([^\n]+)', re.DOTALL),
        "Charge": re.compile(r'TOTAL\s*CHARGE\s*:(.+?)TOTAL', re.DOTALL),
        "Payment": re.compile(r'TOTAL\s*PAYMENT\s*:(.+?)(?=ORIG|\n|$)', re.DOTALL),
        "PAYEE ID": re.compile(r'PAYEE\s*ID\s*:(.+?)AUTH', re.DOTALL),
    }
    
    # Claim reference patterns
    CLAIM_REFERENCE_PATTERNS = {
        "Claim Number": re.compile(r"CLM\s*\#:(.+?)\n", re.DOTALL),
        "Orig Ref Num": re.compile(r'ORIG\s*REF\s*NBR\s*:(.+)\n?', re.DOTALL),
    }
    
    # Header field patterns
    HEADER_FIELD_PATTERNS = {
        "CLAIM STATUS": re.compile(r"CLAIM\s*STATUS\s*:(.+?)\n", re.DOTALL),
        "PAYEE": re.compile(r"PAYEE\s*:(.+?)NPI", re.DOTALL),
        "VENDOR": re.compile(r"VENDOR\s*NBR\s*:(.+?)PROD", re.DOTALL),
        "Pay Date": re.compile(r"PROD\s*DATE\s*:(.+?)CH", re.DOTALL),
        "CHECK/EFT": re.compile(r"CHECK\s*\/\s*EFT\s*NBR\s*:(.+?)CHK", re.DOTALL),
        "CHECK/EFT Date": re.compile(r"CHK\s*\/\s*EFT\s*DT\s*:(.+?)\n", re.DOTALL),
    }
    
    def __init__(self, pdf_path=""):
        """
        Initialize the Medical PDF Claims Parser.
        
        Args:
            pdf_path (str): Path to the PDF file to be processed
        """
        self.pdf_path = pdf_path
        self.patient_regex = re.compile(self.PATIENT_TAG_PATTERN)
        self.patient_response_regex = re.compile(self.PATIENT_RESPONSE_TAG_PATTERN, re.DOTALL)
        
    def calculate_real_page_width(self, page, margin=0):
        """
        Calculate the actual width of content on a PDF page.
        
        This method determines the 'safe' right edge that covers all text 
        and images on the page, accounting for UserUnit scaling.
        
        Args:
            page (fitz.Page): The PDF page object
            margin (float): Optional margin to add to the width
            
        Returns:
            float: The calculated page width including margin
        """
        # Start with mediabox width × user‑unit
        width = page.rect.x1 * getattr(page, "user_unit", 1.0)

        # Find right‑most coordinate of any text block
        try:
            rightmost_text = max(block[2] for block in page.get_text("blocks"))
            width = max(width, rightmost_text)
        except ValueError:  # no text at all
            pass

        return width + margin

    def extract_patient_blocks_from_page(self, current_page, document, page_number, geometry_dict, regex=None):
        """
        Extract individual patient blocks from a single PDF page.
        
        This generator yields patient data blocks by splitting the page content
        at PATIENT tag boundaries.
        
        Args:
            current_page (fitz.Page): Current PDF page object
            document (fitz.Document): The PDF document object
            page_number (int): Current page number
            geometry_dict (dict): Page geometry information for text extraction
            regex (re.Pattern, optional): Regex pattern for patient detection
            
        Yields:
            tuple: Patient block data (text, doc, page_num, page_text, geometry, page)
        """
        if regex is None:
            regex = self.patient_regex
            
        current_page_text = current_page.get_text("text", sort=True)
        
        iterator = regex.finditer(current_page_text)
        first_match = next(iterator, None)
        
        if first_match is None:
            return  # no PATIENT tags found
            
        start_position = first_match.start()
        
        # Process each patient block
        for match in iterator:
            yield (
                current_page_text[start_position:match.start()].lstrip("\n"),
                document,
                page_number,
                current_page_text,
                geometry_dict,
                current_page
            )
            start_position = match.start()
            
        # Yield the final block
        yield (
            current_page_text[start_position:].lstrip("\n"),
            document,
            page_number,
            current_page_text,
            geometry_dict,
            current_page
        )

    def extract_page_geometry_coordinates(self, page, content):
        """
        Extract geometric coordinates for key elements on the page.
        
        This method locates DOS (Date of Service), ADJ/PROD (Adjustment/Product),
        and MOD (Modifier) sections to determine their position coordinates.
        
        Args:
            page (fitz.Page): The PDF page object
            content (str): Page text content
            
        Returns:
            dict: Dictionary containing coordinate information for different sections
        """
        geometry_dict = {}
        
        # Search patterns for key elements
        dos_pattern = ' DOS '
        adjustment_pattern = 'ADJ/PROD'
        modifier_pattern = ' MOD '
        
        # Find DOS coordinates
        dos_instances = page.search_for(dos_pattern)
        if dos_instances:
            dos_rectangle = dos_instances[0]
            geometry_dict['dos_x0'] = int(dos_rectangle.x0) - 30
            geometry_dict['dos_x1'] = int(dos_rectangle.x1) + 30
            geometry_dict['dos_y1'] = int(dos_rectangle.y1)
            
        # Find ADJ/PROD coordinates
        adjustment_instances = page.search_for(adjustment_pattern)
        if adjustment_instances:
            adj_rectangle = adjustment_instances[0]
            geometry_dict['adj_x0'] = int(adj_rectangle.x0) - 2
            geometry_dict['adj_x1'] = int(adj_rectangle.x1) + 10
            
        # Find MOD coordinates
        modifier_instances = page.search_for(modifier_pattern)
        if modifier_instances:
            mod_rectangle = modifier_instances[0]
            geometry_dict['mod_x0'] = int(mod_rectangle.x0) - 20
            geometry_dict['mod_x1'] = int(mod_rectangle.x1) + 20
            
        return geometry_dict

    def iterate_all_patient_blocks(self):
        """
        Main iterator that processes all patient blocks across all PDF pages.
        
        This generator function opens the PDF file and processes each page,
        yielding patient blocks while managing memory efficiently.
        
        Yields:
            tuple: Patient block data for processing
        """
        if not os.path.exists(self.pdf_path):
            print(f'Could not find PDF file at path: {self.pdf_path}')
            return

        geometry_dict = {}
        geometry_extracted = False
        
        with fitz.open(self.pdf_path) as document:
            for page_number, page in enumerate(document):
                # Extract geometry coordinates from first page with PATIENT tags
                if not geometry_extracted:
                    content = page.get_text('text', sort=True)
                    if self.patient_regex.search(content):
                        geometry_dict = self.extract_page_geometry_coordinates(page, content)
                        if geometry_dict:
                            geometry_extracted = True
                
                # Process patient blocks on current page
                yield from self.extract_patient_blocks_from_page(
                    page, document, page_number, geometry_dict
                )

    def extract_claim_area_before_patient_response(self, block_text):
        """
        Extract claim detail lines that appear before "PAT RESP:" marker.
        
        This method identifies and extracts all claim-related lines that contain
        service details, handling both single and multi-line claim sections.
        
        Args:
            block_text (str): The text block to search in
            
        Returns:
            str: Extracted claim area text, or empty string if not found
        """
        # Pattern to capture claim lines starting with 2+ digits
        pattern = re.compile(
            r"(?:REMARK\s*\n?[\s\-]+)\n((?:\d{2,}\s+[\d\-]{4,}\s+[\d\-]{4,}\s+[^\n]*\n).+)(?=PAT\s*RESP\s*:)", 
            re.DOTALL
        )
        
        match = pattern.search(block_text)
        return match.group(1).strip() if match else ""

    def parse_individual_claim_line(self, line):
        """
        Parse a single claim line to extract DOS, HCPCS code, and modifier.
        
        Args:
            line (str): Individual claim line text
            
        Returns:
            tuple: (DOS to-date, HCPCS code, modifier) or empty strings if missing
        """
        tokens = re.split(r"\s+", line.strip())
        if len(tokens) >= 5:
            return tokens[2], tokens[3], tokens[4]
        return "", "", ""

    def parse_complete_claim_area(self, claim_area_text):
        """
        Parse the complete claim area to extract unique DOS, service codes, and modifiers.
        
        Args:
            claim_area_text (str): Text containing all claim lines
            
        Returns:
            dict: Dictionary with 'Date Of Service', 'Service Code', and 'Modifier' keys
        """
        dos_set = set()
        service_code_set = set()
        modifier_set = set()
        
        for line in claim_area_text.splitlines():
            if not line.strip():
                continue
                
            dos, service_code, modifier = self.parse_individual_claim_line(line)
            
            # Add to sets (automatically handles duplicates)
            dos_set.add(dos)
            service_code_set.add(service_code)
            modifier_set.add(modifier)
        
        return {
            'Date Of Service': ','.join(list(dos_set)),
            'Service Code': ','.join(list(service_code_set)),
            'Modifier': ','.join(list(modifier_set))
        }

    def get_remaining_patient_block_content(self, document, current_page_number):
        """
        Extract patient block content that spans multiple pages.
        
        This method handles cases where patient information continues across
        multiple PDF pages by collecting content until the next patient block.
        
        Args:
            document (fitz.Document): The PDF document object
            current_page_number (int): Starting page number
            
        Returns:
            tuple: (crop_rectangles_list, combined_block_text)
        """
        patient_pattern = 'PATIENT:'
        claim_status_pattern = 'CLAIM STATUS'
        bottom_y_coordinate = None
        combined_block_text = ''
        crop_rectangles_list = []
        
        while True:
            current_page_number += 1
            
            if current_page_number >= document.page_count:
                break
                
            page = document.load_page(current_page_number)
            page_width = self.calculate_real_page_width(page)
            
            # Search for PATIENT pattern
            patient_text_instances = page.search_for(patient_pattern)
            
            if len(patient_text_instances) > 0:
                patient_rectangle = patient_text_instances[0]
                bottom_y_coordinate = patient_rectangle.y0
                
                claim_status_instances = page.search_for(claim_status_pattern)
                if claim_status_instances:
                    claim_rectangle = claim_status_instances[0]
                    top_y_coordinate = claim_rectangle.y1
                    crop_rectangle = fitz.Rect(0, top_y_coordinate, page_width, bottom_y_coordinate)
                    crop_rectangles_list.append((page, crop_rectangle))
                    crop_text = page.get_text('text', sort=True, clip=crop_rectangle)
                    combined_block_text += '\n' + re.sub(r'\-{2,}', ' ', crop_text).strip()
                    break
                else:
                    crop_rectangle = fitz.Rect(0, 63, page_width, bottom_y_coordinate)
                    crop_rectangles_list.append((page, crop_rectangle))
                    crop_text = page.get_text('text', sort=True, clip=crop_rectangle)
                    combined_block_text += '\n' + re.sub(r'\-{2,}', ' ', crop_text).strip()
                    break
            else:
                bottom_y_coordinate = page.rect.height
                claim_status_instances = page.search_for(claim_status_pattern)
                
                if claim_status_instances:
                    claim_rectangle = claim_status_instances[0]
                    top_y_coordinate = claim_rectangle.y1
                    crop_rectangle = fitz.Rect(0, top_y_coordinate, page_width, bottom_y_coordinate)
                    crop_rectangles_list.append((page, crop_rectangle))
                    crop_text = page.get_text('text', sort=True, clip=crop_rectangle)
                    combined_block_text += '\n' + re.sub(r'\-{2,}', ' ', crop_text).strip()
                else:
                    crop_rectangle = fitz.Rect(0, 63, page_width, bottom_y_coordinate)
                    crop_rectangles_list.append((page, crop_rectangle))
                    crop_text = page.get_text('text', sort=True, clip=crop_rectangle)
                    combined_block_text += '\n' + re.sub(r'\-{2,}', ' ', crop_text).strip()
        
        return (crop_rectangles_list, combined_block_text)

    def extract_header_field_information(self, page_text):
        """
        Extract header field information from the current page text.
        
        Args:
            page_text (str): Text content of the current page
            
        Returns:
            dict: Dictionary containing extracted header field values
        """
        header_data = {column: "" for column in self.HEADER_FIELD_PATTERNS}
        
        for column, pattern in self.HEADER_FIELD_PATTERNS.items():
            if pattern:
                match = pattern.search(page_text)
                if match:
                    header_data[column] = match.group(1).strip()
                    
        return header_data

    def get_first_block_section(self, page):
        """
        Extract the first section of a patient block from the current page.
        
        Args:
            page (fitz.Page): The PDF page object
            
        Returns:
            tuple: (page, crop_rectangle) or None if not found
        """
        claim_number_regex = re.compile(r"CLM\s*\#:(.+?)\n", re.DOTALL)
        claim_numbers_list = claim_number_regex.findall(page.get_text('text', sort=True))

        page_width = self.calculate_real_page_width(page)
        
        if len(claim_numbers_list) > 0:
            claim_number = claim_numbers_list[-1]
            claim_text_instances = page.search_for(claim_number)
            
            if claim_text_instances:
                claim_rectangle = claim_text_instances[0]
                top_y_coordinate = int(claim_rectangle.y1) + 10
                crop_rectangle = fitz.Rect(0, top_y_coordinate, page_width, page.rect.y1)
                return (page, crop_rectangle)
                
        return None

    def find_next_claim_number(self, claim_list, current_claim_number):
        """
        Find the next claim number in the list after the current one.
        
        Args:
            claim_list (list): List of claim numbers
            current_claim_number (str): Current claim number
            
        Returns:
            str or None: Next claim number or None if current is last
        """
        try:
            index = claim_list.index(current_claim_number)
            if index == len(claim_list) - 1:  # last element?
                return None
            return claim_list[index + 1]
        except ValueError:  # not found
            return None

    def get_bottom_coordinate_by_next_claim(self, page, claim_number):
        """
        Get the bottom Y coordinate by finding the next claim number.
        
        Args:
            page (fitz.Page): The PDF page object
            claim_number (str): Current claim number
            
        Returns:
            int or None: Y coordinate or None if not found
        """
        claim_regex = re.compile(r'CLM\s*\#:(.+?)\n', re.DOTALL)
        claim_numbers = claim_regex.findall(page.get_text('text', sort=True))
        
        if len(claim_numbers) < 2:
            return None

        next_claim = self.find_next_claim_number(claim_numbers, claim_number)
        if not next_claim:
            return None

        rectangles = page.search_for(next_claim)
        return int(rectangles[0].y0) - 20 if rectangles else None

    def find_bottom_of_last_patient_response(self, page, claim_number):
        """
        Find the bottom coordinate of the last patient response section.
        
        Args:
            page (fitz.Page): The PDF page object
            claim_number (str): Claim number to search for
            
        Returns:
            int or None: Bottom Y coordinate or None if not found
        """
        page_width = self.calculate_real_page_width(page)
        claim_hits = page.search_for(claim_number)
        
        if not claim_hits:
            return None
            
        clip_rectangle = fitz.Rect(0, claim_hits[0].y0, page_width, page.rect.y1)
        
        response_hits = page.search_for(
            'PAT RESP:',
            clip=clip_rectangle,
            quads=False,
            flags=fitz.TEXT_DEHYPHENATE | fitz.TEXT_PRESERVE_LIGATURES,
        )
        
        if not response_hits:
            return None

        # Pick the match with the largest y1 → bottom‑most on the page
        return max(rect.y0 for rect in response_hits)

    def create_claim_block_crop_rectangle(self, page, claim_number, original_reference_number):
        """
        Create a crop rectangle for extracting a specific claim block.
        
        Args:
            page (fitz.Page): The PDF page object
            claim_number (str): The claim number to locate
            original_reference_number (str): Original reference number
            
        Returns:
            fitz.Rect or None: Crop rectangle or None if not found
        """
        claim_hits = page.search_for(claim_number)
        page_width = self.calculate_real_page_width(page)
        
        if not claim_hits:
            return None
            
        top_y_coordinate = int(claim_hits[0].y1) + 30  # 30-point padding below claim line
        
        # Find lower boundary
        reference_hits = page.search_for(original_reference_number)
        if reference_hits:
            bottom_y_coordinate = int(reference_hits[0].y0)
        else:
            bottom_y_coordinate = (
                self.get_bottom_coordinate_by_next_claim(page, claim_number) or 
                self.find_bottom_of_last_patient_response(page, claim_number)
            )
            
        if bottom_y_coordinate is None or bottom_y_coordinate <= top_y_coordinate:
            return None

        return fitz.Rect(0, top_y_coordinate, page_width, bottom_y_coordinate)

    def extract_patient_dates_of_service(self, page, patient_rectangle, geometry_dict):
        """
        Extract Date of Service (DOS) values from a patient rectangle area.
        
        Args:
            page (fitz.Page): The PDF page object
            patient_rectangle (fitz.Rect): Rectangle containing patient data
            geometry_dict (dict): Page geometry coordinates
            
        Returns:
            str: Comma-separated DOS values or empty string
        """
        # NEW: Updated method name for clarity
        clip_rectangle = fitz.Rect(
            geometry_dict["dos_x0"],
            patient_rectangle.y0,
            geometry_dict["dos_x1"] - 35,                      
            patient_rectangle.y1,
        )

        # Collect unique values preserving order
        seen_values = set()                                    
        ordered_values = []                                       
        
        for line in page.get_text("text", sort=True, clip=clip_rectangle).splitlines():
            line = line.strip()
            if line and line not in seen_values:                    
                seen_values.add(line)                              
                ordered_values.append(line)                         

        return ",".join(ordered_values) if ordered_values else ""

    def extract_dates_of_service_from_multiple_pages(self, crop_rectangles_list, geometry_dict):
        """
        Extract DOS values from patient data spanning multiple pages.
        
        Args:
            crop_rectangles_list (list): List of (page, rectangle) tuples
            geometry_dict (dict): Page geometry coordinates
            
        Returns:
            str: Comma-separated DOS values
        """
        # NEW: Improved method name and structure
        seen_values = set() 
        ordered_values = [] 
        
        if not crop_rectangles_list:
            return ""
            
        first_page_data, *middle_pages_data, last_page_data = crop_rectangles_list
        
        # Process first page
        first_page, first_rectangle = first_page_data
        first_dos_rectangle = fitz.Rect(
            geometry_dict['dos_x0'], 
            first_rectangle.y0 + 20, 
            geometry_dict['dos_x1'] - 35, 
            first_rectangle.y1
        )
        first_dos_text = first_page.get_text('text', sort=True, clip=first_dos_rectangle)
        
        for line in first_dos_text.splitlines():
            line = line.strip()
            if line and line not in seen_values:                    
                seen_values.add(line)                              
                ordered_values.append(line) 

        # Process middle pages
        for middle_page_data in middle_pages_data:
            middle_page, middle_rectangle = middle_page_data
            middle_dos_rectangle = fitz.Rect(
                geometry_dict['dos_x0'], 
                middle_rectangle.y0 + 20, 
                geometry_dict['dos_x1'] - 35, 
                middle_rectangle.y1
            )
            middle_dos_text = middle_page.get_text('text', sort=True, clip=middle_dos_rectangle)
            
            for line in middle_dos_text.splitlines():
                line = line.strip()
                if line and line not in seen_values:                    
                    seen_values.add(line)                              
                    ordered_values.append(line) 

        # Process last page
        last_page, last_rectangle = last_page_data
        last_dos_rectangle = fitz.Rect(
            geometry_dict['dos_x0'], 
            last_rectangle.y0 + 20, 
            geometry_dict['dos_x1'] - 35, 
            last_rectangle.y1 - 20
        )
        last_dos_text = last_page.get_text('text', sort=True, clip=last_dos_rectangle)
        
        for line in last_dos_text.splitlines():
            line = line.strip()
            if line and line not in seen_values:                    
                seen_values.add(line)                              
                ordered_values.append(line) 
        
        return ",".join(ordered_values)

    def extract_patient_service_codes(self, page, patient_rectangle, geometry_dict):
        """
        Extract service codes from a patient rectangle area.
        
        Args:
            page (fitz.Page): The PDF page object
            patient_rectangle (fitz.Rect): Rectangle containing patient data
            geometry_dict (dict): Page geometry coordinates
            
        Returns:
            str: Comma-separated service codes
        """
        service_code_set = set()
        service_code_rectangle = fitz.Rect(
            geometry_dict['adj_x0'], 
            patient_rectangle.y0, 
            geometry_dict['adj_x1'], 
            patient_rectangle.y1
        )
        service_code_text = page.get_text('text', sort=True, clip=service_code_rectangle)
        
        for line in service_code_text.splitlines():
            if line.strip():
                service_code_set.add(line.strip())
                
        return ','.join(list(service_code_set))

    def extract_service_codes_from_multiple_pages(self, crop_rectangles_list, geometry_dict):
        """
        Extract service codes from patient data spanning multiple pages.
        
        Args:
            crop_rectangles_list (list): List of (page, rectangle) tuples
            geometry_dict (dict): Page geometry coordinates
            
        Returns:
            str: Comma-separated service codes
        """
        # NEW: Improved method name and structure
        seen_values = set() 
        ordered_values = [] 
        
        if not crop_rectangles_list:
            return ""
            
        first_page_data, *middle_pages_data, last_page_data = crop_rectangles_list
        
        # Process first page
        first_page, first_rectangle = first_page_data
        first_service_rectangle = fitz.Rect(
            geometry_dict['adj_x0'], 
            first_rectangle.y0 + 20, 
            geometry_dict['adj_x1'], 
            first_rectangle.y1
        )
        first_service_text = first_page.get_text('text', sort=True, clip=first_service_rectangle)
        
        for line in first_service_text.splitlines():
            line = line.strip()
            if line and line not in seen_values:                    
                seen_values.add(line)                              
                ordered_values.append(line) 

        # Process middle pages
        for middle_page_data in middle_pages_data:
            middle_page, middle_rectangle = middle_page_data
            middle_service_rectangle = fitz.Rect(
                geometry_dict['adj_x0'], 
                middle_rectangle.y0 + 20, 
                geometry_dict['adj_x1'], 
                middle_rectangle.y1
            )
            middle_service_text = middle_page.get_text('text', sort=True, clip=middle_service_rectangle)
            
            for line in middle_service_text.splitlines():
                line = line.strip()
                if line and line not in seen_values:                    
                    seen_values.add(line)                              
                    ordered_values.append(line) 

        # Process last page
        last_page, last_rectangle = last_page_data
        last_service_rectangle = fitz.Rect(
            geometry_dict['adj_x0'], 
            last_rectangle.y0 + 20, 
            geometry_dict['adj_x1'], 
            last_rectangle.y1 - 20
        )
        last_service_text = last_page.get_text('text', sort=True, clip=last_service_rectangle)
        
        for line in last_service_text.splitlines():
            line = line.strip()
            if line and line not in seen_values:                    
                seen_values.add(line)                              
                ordered_values.append(line) 
        
        return ",".join(ordered_values)

    def extract_patient_modifiers(self, page, patient_rectangle, geometry_dict):
        """
        Extract modifier codes from a patient rectangle area.
        
        Args:
            page (fitz.Page): The PDF page object
            patient_rectangle (fitz.Rect): Rectangle containing patient data
            geometry_dict (dict): Page geometry coordinates
            
        Returns:
            str: Comma-separated modifier codes
        """
        modifier_set = set()
        modifier_rectangle = fitz.Rect(
            geometry_dict['mod_x0'], 
            patient_rectangle.y0, 
            geometry_dict['mod_x1'], 
            patient_rectangle.y1
        )
        modifier_text = page.get_text('text', sort=True, clip=modifier_rectangle)
        
        for line in modifier_text.splitlines():
            if line.strip():
                modifier_set.add(line.strip())
                
        return ','.join(list(modifier_set))

    def extract_modifiers_from_multiple_pages(self, crop_rectangles_list, geometry_dict):
        """
        Extract modifier codes from patient data spanning multiple pages.
        
        Args:
            crop_rectangles_list (list): List of (page, rectangle) tuples
            geometry_dict (dict): Page geometry coordinates
            
        Returns:
            str: Comma-separated modifier codes
        """
        # NEW: Improved method name and structure
        seen_values = set() 
        ordered_values = [] 
        
        if not crop_rectangles_list:
            return ""
            
        first_page_data, *middle_pages_data, last_page_data = crop_rectangles_list
        
        # Process first page
        first_page, first_rectangle = first_page_data
        first_modifier_rectangle = fitz.Rect(
            geometry_dict['mod_x0'], 
            first_rectangle.y0 + 20, 
            geometry_dict['mod_x1'], 
            first_rectangle.y1
        )
        first_modifier_text = first_page.get_text('text', sort=True, clip=first_modifier_rectangle)
        
        for line in first_modifier_text.splitlines():
            line = line.strip()
            if line and line not in seen_values:                    
                seen_values.add(line)                              
                ordered_values.append(line) 

        # Process middle pages
        for middle_page_data in middle_pages_data:
            middle_page, middle_rectangle = middle_page_data
            middle_modifier_rectangle = fitz.Rect(
                geometry_dict['mod_x0'], 
                middle_rectangle.y0 + 20, 
                geometry_dict['mod_x1'], 
                middle_rectangle.y1
            )
            middle_modifier_text = middle_page.get_text('text', sort=True, clip=middle_modifier_rectangle)
            
            for line in middle_modifier_text.splitlines():
                line = line.strip()
                if line and line not in seen_values:                    
                    seen_values.add(line)                              
                    ordered_values.append(line) 

        # Process last page
        last_page, last_rectangle = last_page_data
        last_modifier_rectangle = fitz.Rect(
            geometry_dict['mod_x0'], 
            last_rectangle.y0 + 20, 
            geometry_dict['mod_x1'], 
            last_rectangle.y1 - 20
        )
        last_modifier_text = last_page.get_text('text', sort=True, clip=last_modifier_rectangle)
        
        for line in last_modifier_text.splitlines():
            line = line.strip()
            if line and line not in seen_values:                    
                seen_values.add(line)                              
                ordered_values.append(line) 
            
        return ",".join(ordered_values)

    def process_individual_patient_block(self, block_tuple):
        """
        Process an individual patient block to extract all relevant information.
        
        This is the main processing method that handles both single-page and 
        multi-page patient blocks, extracting all fields and service details.
        
        Args:
            block_tuple (tuple): Contains (block_text, document, page_number, 
                                 page_text, geometry_dict, current_page)
            
        Returns:
            list: List of dictionaries containing extracted patient data
        """
        # Initialize base data structure
        base_data = {column: "" for column in self.FIELD_EXTRACTION_PATTERNS}
        
        # Unpack block tuple
        block_text = block_tuple[0]
        document = block_tuple[1]
        current_page_number = block_tuple[2]
        current_page_text = block_tuple[3]
        geometry_dict = block_tuple[4]
        current_page = block_tuple[5]

        crop_rectangles_list = []
        
        # Check if patient response marker exists
        response_regex_match = self.patient_response_regex.search(block_text, re.DOTALL)
        
        if not response_regex_match:
            # Handle multi-page patient block
            first_rectangle_block = self.get_first_block_section(current_page)
            
            if first_rectangle_block:
                crop_rectangles_list.append(first_rectangle_block)
                
            rectangle_list, remaining_block_text = self.get_remaining_patient_block_content(
                document, current_page_number
            )
            crop_rectangles_list.extend(rectangle_list)
            block_text += '\n' + remaining_block_text
            
            # Extract claim reference information
            for column, pattern in self.CLAIM_REFERENCE_PATTERNS.items():
                if pattern:
                    match = pattern.search(block_text)
                    base_data[column] = match.group(1).strip() if match else ''
            
            # Extract service information from multiple pages
            claim_number = base_data['Claim Number']
            
            dates_of_service = self.extract_dates_of_service_from_multiple_pages(
                crop_rectangles_list, geometry_dict
            )
            base_data['Date Of Service'] = dates_of_service
            
            service_codes = self.extract_service_codes_from_multiple_pages(
                crop_rectangles_list, geometry_dict
            )
            base_data['Service Code'] = service_codes

            modifiers = self.extract_modifiers_from_multiple_pages(
                crop_rectangles_list, geometry_dict
            )
            base_data['Modifier'] = modifiers

        else:
            # Handle single-page patient block
            for column, pattern in self.CLAIM_REFERENCE_PATTERNS.items():
                if pattern:
                    match = pattern.search(block_text)
                    base_data[column] = match.group(1).strip() if match else ''
            
            # Create crop rectangle for the claim block
            block_rectangle = self.create_claim_block_crop_rectangle(
                current_page, 
                base_data['Claim Number'], 
                base_data['Orig Ref Num']
            )
            
            if block_rectangle:
                # Extract service information from single page
                dates_of_service = self.extract_patient_dates_of_service(
                    current_page, block_rectangle, geometry_dict
                )
                base_data['Date Of Service'] = dates_of_service
                
                service_codes = self.extract_patient_service_codes(
                    current_page, block_rectangle, geometry_dict
                )
                base_data['Service Code'] = service_codes
                
                modifiers = self.extract_patient_modifiers(
                    current_page, block_rectangle, geometry_dict
                )
                base_data['Modifier'] = modifiers
            else:
                print(f'Could not find block rectangle for claim number {base_data["Claim Number"]}')

        # Extract header field information
        for column, pattern in self.FIELD_EXTRACTION_PATTERNS.items():
            if pattern:
                match = pattern.search(block_text)
                if match:
                    base_data[column] = match.group(1).strip()

        # Extract and merge header information
        header_data = self.extract_header_field_information(current_page_text)
        final_data = base_data.copy()
        final_data.update(header_data)
        
        claim_number = base_data['Claim Number']
        print(f'Processing Patient Block - Claim Number: {claim_number}')

        return [final_data]

    def process_pdf_and_create_dataframe(self):
        """
        Process the entire PDF and create a pandas DataFrame with extracted data.
        
        This method orchestrates the complete processing workflow, from reading
        the PDF to creating the final structured output.
        
        Returns:
            pandas.DataFrame: DataFrame containing all extracted patient data
        """
        print(f"Starting PDF processing: {self.pdf_path}")
        start_time = time.perf_counter()
        
        all_rows = []
        
        # Process each patient block
        for block_number, patient_block in enumerate(self.iterate_all_patient_blocks(), start=1):
            processed_rows = self.process_individual_patient_block(patient_block)
            all_rows.extend(processed_rows)
        
        # Create DataFrame
        if all_rows:
            dataframe = pandas.DataFrame(all_rows, columns=all_rows[0].keys())
        else:
            # Create empty DataFrame with expected columns
            expected_columns = list(self.FIELD_EXTRACTION_PATTERNS.keys()) + \
                             list(self.CLAIM_REFERENCE_PATTERNS.keys()) + \
                             list(self.HEADER_FIELD_PATTERNS.keys()) + \
                             ['Date Of Service', 'Service Code', 'Modifier']
            dataframe = pandas.DataFrame(columns=expected_columns)
        
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        elapsed_minutes = elapsed_time / 60
        
        print(f"Processing completed in {elapsed_time:.6f} seconds ({elapsed_minutes:.2f} minutes)")
        print(f"Extracted {len(all_rows)} patient records")
        print(f"DataFrame shape: {dataframe.shape}")
        
        return dataframe

    def save_results_to_excel(self, dataframe, output_path=None):
        """
        Save the processed results to an Excel file.
        
        Args:
            dataframe (pandas.DataFrame): The processed data
            output_path (str, optional): Custom output path. If None, uses input filename
        """
        # NEW: Auto-generate output filename from input filename
        if output_path is None:
            input_path = Path(self.pdf_path)
            output_path = input_path.with_suffix('.xlsx')
        
        # Define column order for output
        column_order = [
            'Patient Name', 'Patient ID', 'Provider Name', 'Provider ID',
            'CLAIM STATUS', 'Claim Number', 'Orig Ref Num', 'Patient CTRL',
            'Provider CTRL', 'Date Of Service', 'Service Code', 'Modifier',
            'Charge', 'Payment', 'PAYEE', 'PAYEE ID', 'VENDOR', 'Pay Date',
            'CHECK/EFT', 'CHECK/EFT Date',
        ]
        
        # Reorder columns (only include columns that exist in the dataframe)
        available_columns = [col for col in column_order if col in dataframe.columns]
        ordered_dataframe = dataframe[available_columns]
        
        # Save to Excel
        ordered_dataframe.to_excel(output_path, index=False)
        print(f'Results saved to: {output_path}')

    def run_complete_processing(self, output_path=None):
        """
        Run the complete PDF processing workflow.
        
        This is the main entry point that handles the entire process from
        PDF reading to Excel output generation.
        
        Args:
            output_path (str, optional): Custom output path for Excel file
            
        Returns:
            pandas.DataFrame: The processed data
        """
        try:
            # Process PDF and create DataFrame
            dataframe = self.process_pdf_and_create_dataframe()
            
            # Save results to Excel
            self.save_results_to_excel(dataframe, output_path)
            
            return dataframe
            
        except Exception as error:
            print(f"Error during processing: {str(error)}")
            raise

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"})

@app.route('/parse-pdf', methods=['POST'])
def parse_pdf_endpoint():
    try:
        if 'pdf_file' not in request.files:
            return jsonify({"error": "No PDF file provided"}), 400
        
        pdf_file = request.files['pdf_file']
        if pdf_file.filename == '':
            return jsonify({"error": "No file selected"}), 400
        
        # Save uploaded file temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_pdf:
            pdf_file.save(temp_pdf.name)
            
            # Create parser instance with temp file
            parser = MedicalPDFClaimsParser(temp_pdf.name)
            
            # Process PDF
            results_dataframe = parser.process_pdf_and_create_dataframe()
            
            # Convert to CSV
            csv_buffer = io.StringIO()
            results_dataframe.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            # Clean up temp file
            os.unlink(temp_pdf.name)
            
            # Return CSV content
            output = io.BytesIO()
            output.write(csv_content.encode('utf-8'))
            output.seek(0)
            
            return send_file(
                output,
                mimetype='text/csv',
                as_attachment=True,
                download_name=f"{pdf_file.filename.rsplit('.', 1)[0]}_parsed.csv"
            )
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)