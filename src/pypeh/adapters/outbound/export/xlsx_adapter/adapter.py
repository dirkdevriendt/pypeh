"""
A reusable export adapter that exports templated data, empty templates or data dictionaries as xlsx files
"""

from __future__ import annotations

import logging

from typing import TYPE_CHECKING

from pypeh.core.cache.dataview import DataView
from pypeh.core.interfaces.outbound.dataops import ExportInterface, ExportTypeEnum
from pypeh.adapters.outbound.validation.pandera_adapter.parsers import parse_config, parse_error_report
from pypeh.core.models.validation_errors import ValidationReport

import xlsxwriter

if TYPE_CHECKING:
    from typing import Mapping, Sequence

logger = logging.getLogger(__name__)

ANALYTICALINFO_EXCLUSION_LIST = ["id_subject", "matrix", "analysisyear", "analysismonth", "analysisday", "density", "osm", "sg", "uvolume"]
ANALYTICALINFO_MATRIX_TRANSLATION = {"urine_lab": "US;UM", "bloodserum_lab": "BS", "bloodwholeblood_lab":"BWB"}
STUDYINFO_HEADERS = ["THIS INFORMATION IS PROVIDED BY PARC DATA MANAGEMENT TEAM", "...2", "...3"]
CODEBOOK_METADATA = {"Codebook Reference": f"PARCAlignedStudies_adults_v2.4", "Codebook Name": f"PARCAlignedStudies_adults", "Codebook Version": "2.4"}

DATATYPE_TRANSLATION_DICT = {
    "string": "varchar",
    "number": "decimal",
    "boolean": "bool",
    "datetime": "datetime",
}

def get_observable_property_property(varname, observable_property, codebook_property_name):
    match str(codebook_property_name):
        case "DataRequestCategory":
            return ";\r\n".join([g for g in observable_property.grouping_id_list]) if observable_property.grouping_id_list else None
        case "Varname":
            return varname
        case "Description":
            return observable_property.description if observable_property.description else observable_property.label
        case "Type":
            if observable_property.categorical:
                return "categorical"
            return DATATYPE_TRANSLATION_DICT[observable_property.value_type]
        case "Unit":
            return None
        case "MissingsAllowed":
            return 0 if observable_property.default_required else 1
        case "MinValue":
            return None
        case "MaxValue":
            return None
        case "AllowedValues":
            if observable_property.categorical:
                return ";\r\n".join([vo.key + " = " + vo.value for vo in observable_property.value_options])
            else:
                return None 
        case "DecimalsAfterComma":
            return None
        case "Conditional":
            return None
        case "Formula":
            return None
        case "Remarks":
            return None

def fill_excel_form_sheet(worksheet, style_dict, header_list=None, metadata_record_dict=None, autofit=True):
    for counter, header in enumerate(header_list):
        worksheet.write(0, counter, header, style_dict["header"])
    for counter, metadata_record_key in enumerate(metadata_record_dict.keys()):
        worksheet.write(counter+1, 0, metadata_record_key)
        worksheet.write(counter+1, 1, metadata_record_dict[metadata_record_key])
    if autofit:
        worksheet.autofit()

def fill_excel_worksheet_from_section(worksheet, section, observable_property_dict, style_dict, observed_values = None, data_list=None, autofit=True):
    match str(section.section_type):
        case "data_form":
            row = 0
            for element in section.elements:
                match str(element.element_type):
                    case "spacer":
                        pass
                    case "text":
                        worksheet.write(row, 0, element.label, style_dict[str(element.element_style)] if str(element.element_style) in style_dict.keys() else None)
                    case "data_field":
                        worksheet.write(row, 0, element.label)
                        # default value: worksheet.write(row, 1, observable_property.default_value)
                row += 1
        case "data_table":
            column_ids = [element.varname for element in section.elements]
            for c_nr, c_name in enumerate(column_ids):
                worksheet.write(0, c_nr, c_name, style_dict["header"])
            if data_list is not None and isinstance(data_list, list):
                for r_nr, record in enumerate(data_list):
                    for c_nr, element in enumerate(record):
                        worksheet.write(r_nr + 1, c_nr, element)
            if observed_values is not None and isinstance(observed_values, list):
                row_ids = list(set(observed_value.observable_entity for observed_value in observed_values))
                for r_nr, r_name in enumerate(row_ids):
                    for c_nr, c_name in enumerate(column_ids):
                        worksheet.write(r_nr + 1, c_nr, [observed_value.value_as_string for observed_value in observed_values if observed_value.observable_entity==r_name and observed_value.observable_property==c_name][0])
        case "property_table":
            columns = ["DataRequestCategory", "Varname", "Description", "Type", "Unit", "MissingsAllowed", "MinValue", "MaxValue", "AllowedValues", "DecimalsAfterComma", "Conditional", "Formula", "Remarks"]
            for c_nr, c_name in enumerate(columns):
                worksheet.write(0, c_nr, c_name, style_dict["bold"])
            row = 1
            for element in section.elements:
                index_name = element.observable_property
                if index_name.endswith("_lod") or index_name.endswith("_loq"):
                    index_name = index_name[:-4]
                if index_name not in observable_property_dict and f"mass concentration of {index_name} in urine" in observable_property_dict:
                    index_name = f"mass concentration of {index_name} in urine"
                op = observable_property_dict[index_name]

                for c_nr, c_name in enumerate(columns):
                    worksheet.write(row, c_nr, get_observable_property_property(element.varname, op, c_name))
                row += 1
    if autofit:
        worksheet.autofit()

def write_excel_datatemplate(data_layout, path, observable_property_dict=None, studyinfo_header_list=None, codebook_metadata_dict=None):
    if studyinfo_header_list is None:
        studyinfo_header_list = STUDYINFO_HEADERS
    if codebook_metadata_dict is None:
        codebook_metadata_dict = CODEBOOK_METADATA
    
    def create_analyticalinfo_dataset(data_layout):
        dataset = []
        for section in data_layout.sections:
            matrix = ANALYTICALINFO_MATRIX_TRANSLATION.get(section.label)
            if matrix:
                dataset.extend([(element.varname, matrix) for element in section.elements if not(element.varname in ANALYTICALINFO_EXCLUSION_LIST or element.varname[-4:] in ["_lod", "_loq"])])
        return dataset
    
    workbook = xlsxwriter.Workbook(path)
    style_dict = {
        "header": workbook.add_format({"font_color": "white", "bg_color": "#4F80BD", "bold": True}),
        "warning": workbook.add_format({"font_color": "red"}),
        "bold": workbook.add_format({"bold": True}),
    }
    worksheet = workbook.add_worksheet("studyinfo")
    worksheet.autofit()
    fill_excel_form_sheet(worksheet, style_dict, header_list=studyinfo_header_list, metadata_record_dict=codebook_metadata_dict)
    for section in data_layout.sections:
        worksheet = workbook.add_worksheet(section.label)
        data_list = create_analyticalinfo_dataset(data_layout) if section.label=="analyticalinfo" else None
        fill_excel_worksheet_from_section(worksheet, section, observable_property_dict, style_dict, data_list=data_list)
    workbook.close()

class XlsxExportAdapter(ExportInterface):
    """
    the XlsxExportAdapter implements ExportInterface, which provides an export method that can be called with Mapping objects and a filepath as target:
    `self.export(export_type, data, config, target_path)`
    It also inherits from a DataOpsInterface, which has a process method that can be called like this:
    `self.process(dto, "export")`
    """

    def export(self, export_type: ExportTypeEnum = ExportTypeEnum.EmptyDataTemplate,
               data: Mapping = None, config: DataView = None, observable_property_dict = None,
               data_layout_id: str = None, data_extract_id: str = None, target_path: str = None):
        match export_type:
            case ExportTypeEnum.EmptyDataTemplate:
                data_layout = config.request_entity(data_layout_id, "DataLayout")
                write_excel_datatemplate(data_layout, target_path, observable_property_dict=observable_property_dict)
            case _:
                raise NotImplementedError