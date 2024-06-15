from __future__ import annotations
import re, pathlib
from typing import TypedDict, NotRequired

class RequirementSpec(TypedDict):
  """A Python Package Requirement Spec"""
  name: str
  """The Name of the Package"""
  extras: NotRequired[list[str]]
  """The Extras for the Package"""
  versions: NotRequired[list[str]]
  """The Version Specifiers for the Package"""
  url: NotRequired[str]
  """The URL for the Package"""
  marker: NotRequired[str]
  """The Marker for the Package"""

  @staticmethod
  def validate(spec: RequirementSpec):
    if not isinstance(spec, dict): raise ValueError("RequirementSpec must be a dictionary")
    if 'name' not in spec: raise ValueError("RequirementSpec must contain a 'name' key")
    if not isinstance(spec['name'], str): raise ValueError("RequirementSpec 'name' must be a string")
    if 'extras' in spec:
      if not isinstance(spec['extras'], list): raise ValueError("RequirementSpec 'extras' must be a list")
      if not all(isinstance(e, str) for e in spec['extras']): raise ValueError("RequirementSpec 'extras' must be a list of strings")
    if 'versions' in spec:
      if not isinstance(spec['versions'], list): raise ValueError("RequirementSpec 'versions' must be a list")
      if not all(isinstance(v, str) for v in spec['versions']): raise ValueError("RequirementSpec 'versions' must be a list of strings")
    if 'url' in spec:
      if not isinstance(spec['url'], str): raise ValueError("RequirementSpec 'url' must be a string")
    if 'marker' in spec:
      if not isinstance(spec['marker'], str): raise ValueError("RequirementSpec 'marker' must be a string")

  @staticmethod
  def parse_requirement_line(requirement: str) -> RequirementSpec:
    # Define regex patterns
    name_pattern = r'^[a-zA-Z0-9][a-zA-Z0-9._-]*'
    extras_pattern = r'\[([a-zA-Z0-9_, ]+)\]'
    version_pattern = r'((?:[><=!~]=?|\===)\s*[^,;\s]+(?:\s*,\s*[><=!~]=?\s*[^,;\s]+)*)'
    url_pattern = r'@ ([^;\s]+)'
    marker_pattern = r';\s*(.+)$'
    
    # Compile the full pattern
    full_pattern = fr'^\s*({name_pattern})(?:{extras_pattern})?(?:\s*{version_pattern})?(?:\s*{url_pattern})?(?:\s*{marker_pattern})?\s*$'
    
    # Match the pattern against the requirement line
    match = re.match(full_pattern, requirement)
    if not match:
      raise ValueError("Invalid requirement line format")
    
    # Extract matched groups
    name = match.group(1)
    extras = match.group(2)
    versions = match.group(3)
    url = match.group(4)
    marker = match.group(5)
    
    # Process extras into a list
    extras_list = [e.strip() for e in extras.split(',')] if extras else []
    
    # Process versions into a list
    versions_list = [v.strip() for v in versions.split(',')] if versions else []
    
    # Create the result dictionary
    return { k: v for k, v in {
      'name': name,
      'extras': extras_list,
      'versions': versions_list,
      'url': url,
      'marker': marker
    }.items() if v }

  @staticmethod
  def render_requirement_line(spec: RequirementSpec):
    name = spec.get('name', '')
    extras = spec.get('extras', [])
    versions = spec.get('versions', [])
    url = spec.get('url', None)
    marker = spec.get('marker', None)
    
    # Construct the extras part
    extras_part = f"[{','.join(extras)}]" if extras else ""
    
    # Construct the versions part
    versions_part = ", ".join(versions) if versions else ""
    
    # Construct the URL part
    url_part = f" @ {url}" if url else ""
    
    # Construct the marker part
    marker_part = f"; {marker}" if marker else ""
    
    # Combine all parts into a single requirement line
    requirement_line = f"{name}{extras_part} {versions_part}{url_part}{marker_part}".strip()
    
    return requirement_line

def parse_requirements_file(file: pathlib.Path) -> dict[str, RequirementSpec]:
  requirements = {}
  with file.open('r') as f:
    for line in f:
      line = line.strip()
      if not line or line.startswith('#'):
        continue
      spec = RequirementSpec.parse_requirement_line(line)
      if spec['name'] in requirements: requirements[spec['name']] |= spec
      else: requirements[spec['name']] = spec
  return requirements
