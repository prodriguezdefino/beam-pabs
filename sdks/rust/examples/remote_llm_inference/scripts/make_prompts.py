#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""Builds the deterministic LLM benchmark prompt set from the public Shakespeare corpus.

Each prompt asks the model to explain one speech from a play. The speeches are
sampled evenly across all plays, so the same corpus always gives the same file.
One prompt is written per line; both the Rust and the Python pipelines send each
line to the model verbatim. `kinglear-hashtag.txt` is a hashtagged copy of
`kinglear.txt` and is left out.

  gcloud storage cp 'gs://apache-beam-samples/shakespeare/*.txt' /tmp/shakespeare/
  rm /tmp/shakespeare/kinglear-hashtag.txt
  python make_prompts.py /tmp/shakespeare/*.txt --count=1000 > llm_prompts_1k.txt
  gcloud storage cp llm_prompts_1k.txt \
    gs://<bucket>/datasets/llm_prompts_1k.txt
"""

import argparse
import dataclasses
import pathlib
import sys
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Sequence

PROMPT_TEMPLATE = (
    "Explain in one sentence of plain modern English what {speaker} means in "
    "this passage from Shakespeare's {title}: {text}")
SETTING_PREFIX = "SCENE\t"


@dataclasses.dataclass(frozen=True)
class Speech:
  title: str
  speaker: str
  text: str

  def prompt(self) -> str:
    return PROMPT_TEMPLATE.format(
        speaker=self.speaker, title=self.title, text=self.text)


def blocks(lines: Iterable[str]) -> Iterator[list[str]]:
  """Groups lines into blank-line separated blocks."""
  block: list[str] = []
  for line in lines:
    if line.strip():
      block.append(line.rstrip())
    elif block:
      yield block
      block = []
  if block:
    yield block


def speech(title: str, block: Sequence[str], min_lines: int,
           max_chars: int) -> Speech | None:
  """Parses a `SPEAKER<TAB>line` block, skipping stage directions."""
  speaker, sep, first = block[0].partition("\t")
  if not sep or not speaker.isupper():
    return None
  verse = [first.strip()] + [line.strip() for line in block[1:]]
  verse = [line for line in verse if line and not line.startswith("[")]
  text = " ".join(verse)
  if len(verse) < min_lines or len(text) > max_chars or "|" in text:
    return None
  return Speech(title=title, speaker=speaker.title(), text=text)


def speeches(path: pathlib.Path, min_lines: int,
             max_chars: int) -> Iterator[Speech]:
  """Yields the speeches of a play; poems, which have no setting, yield none.

  The dramatis personae ends at the `SCENE<TAB>setting` line; the dialogue
  follows it.
  """
  lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
  title = next(line.strip() for line in lines if line.strip())
  setting = next(
      (i for i, line in enumerate(lines) if line.startswith(SETTING_PREFIX)),
      None)
  if setting is None:
    return
  for block in blocks(lines[setting + 1:]):
    parsed = speech(title, block, min_lines, max_chars)
    if parsed is not None:
      yield parsed


def sample_evenly(items: Sequence[Speech], count: int) -> list[Speech]:
  if count > len(items):
    raise ValueError(f"Asked for {count} prompts, corpus has {len(items)}")
  return [items[i * len(items) // count] for i in range(count)]


def main(argv: Sequence[str]) -> None:
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("corpus", nargs="+", type=pathlib.Path)
  parser.add_argument("--count", type=int, default=1000)
  parser.add_argument("--min_lines", type=int, default=2)
  parser.add_argument("--max_chars", type=int, default=600)
  args = parser.parse_args(argv)

  corpus = [
      s for path in sorted(args.corpus)
      for s in speeches(path, args.min_lines, args.max_chars)
  ]
  for s in sample_evenly(corpus, args.count):
    sys.stdout.write(s.prompt() + "\n")


if __name__ == "__main__":
  main(sys.argv[1:])
