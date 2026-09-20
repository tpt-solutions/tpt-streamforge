// tpt-streamforge browser playground: builds a pipeline from the stage list
// and runs it locally in WebAssembly.
import { Pipeline } from '../index.js';

const SAMPLE = [
  'order,region,amount,day',
  '1001,north,120.50,2024-01-15',
  '1002,south,80.00,2024-01-16',
  '1003,north,240.00,2024-02-01',
  '1004,east,35.25,2024-02-03',
  '1005,south,80.00,2024-02-10',
  '1006,west,150.75,2024-02-12',
  '1007,north,90.00,2024-03-01',
].join('\n') + '\n';

const AGG_FNS = ['sum', 'avg', 'count', 'count_all', 'min', 'max'];

const stageList = document.getElementById('stages');
const input = document.getElementById('input');
const status = document.getElementById('status');
const output = document.getElementById('output');
const newOp = document.getElementById('new-op');

input.value = SAMPLE;

function el(tag, attrs = {}, text = '') {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  node.textContent = text;
  return node;
}

function makeStage(op) {
  const row = el('div', { class: 'stage' });
  row.appendChild(el('span', { class: 'op-name' }, op));
  const fields = [];

  const addField = (placeholder, value = '', cls = '') => {
    const f = el('input', { placeholder, class: cls });
    f.value = value;
    fields.push(f);
    row.appendChild(f);
    return f;
  };

  if (op === 'filter') {
    addField('amount > 0', 'amount > 0', 'expr');
  } else if (op === 'map') {
    addField('name', '', '');
    addField('= expr', '', 'expr');
    row.insertBefore(document.createTextNode('='), row.lastChild);
  } else if (op === 'sort') {
    addField('col1, col2 (ascending)', '', 'expr');
  } else if (op === 'dedup') {
    addField('col1, col2', '', 'expr');
  } else if (op === 'aggregate') {
    addField('group by (comma-separated)', '', 'group');
    addField('name = sum(col)', '', 'expr');
  }

  const remove = el('button', { class: 'remove', title: 'Remove stage' });
  remove.textContent = '✕';
  remove.addEventListener('click', () => row.remove());
  row.appendChild(remove);

  row.getSpec = () => {
    const get = (i) => fields[i].value.trim();
    switch (op) {
      case 'filter':
        return (p) => p.filter(get(0));
      case 'map':
        return (p) => p.map([get(0)], [get(1)]);
      case 'sort':
        return (p) =>
          p.sort(
            get(0).split(',').map((s) => s.trim()).filter(Boolean),
            false,
          );
      case 'dedup':
        return (p) =>
          p.dedup(get(0).split(',').map((s) => s.trim()).filter(Boolean));
      case 'aggregate': {
        return (p) => {
          const groups = get(0)
            .split(',')
            .map((s) => s.trim())
            .filter(Boolean);
          const [name, fnCall] = get(1).split('=').map((s) => s.trim());
          const match = /^(sum|avg|count_all|count|min|max)\s*\(\s*([^\s)]+)?\s*\)$/i.exec(
            fnCall ?? '',
          );
          if (!match) throw new Error('aggregate spec must look like: total = sum(amount)');
          const fn = match[1].toLowerCase();
          const col = match[2] ?? '';
          if (fn === 'count_all' || col === '') {
            return p.aggregate(groups, ['count_all'], ['']);
          }
          return p.aggregate(groups, [fn], [col]);
        };
      }
      default:
        return (p) => p;
    }
  };
  return row;
}

document.getElementById('add').addEventListener('click', () => {
  stageList.appendChild(makeStage(newOp.value));
  newOp.selectedIndex = 0;
});

function renderError(message) {
  status.textContent = message;
  status.classList.add('error');
  output.replaceChildren();
}

function renderTable(rows) {
  status.classList.remove('error');
  const names = rows.columnNames();
  const count = rows.numRows();
  const json = rows.toJSON();

  status.textContent = `${count} row(s), ${names.length} column(s)`;
  const table = el('table');
  const head = el('thead');
  const headRow = el('tr');
  for (const name of names) headRow.appendChild(el('th', {}, name));
  head.appendChild(headRow);
  table.appendChild(head);
  const body = el('tbody');
  const shown = json.slice(0, 100);
  for (const record of shown) {
    const tr = el('tr');
    for (const name of names) tr.appendChild(el('td', {}, String(record[name] ?? '')));
    body.appendChild(tr);
  }
  table.appendChild(body);
  output.replaceChildren(table);
  if (json.length > shown.length) {
    output.appendChild(
      el('p', {}, `… ${json.length - shown.length} more rows (showing first 100)`),
    );
  }
}

document.getElementById('run').addEventListener('click', () => {
  try {
    let pipeline = new Pipeline(input.value, 0);
    for (const row of stageList.children) {
      if (typeof row.getSpec === 'function') {
        pipeline = row.getSpec()(pipeline);
      }
    }
    renderTable(pipeline);
  } catch (err) {
    renderError(`error: ${err.message ?? err}`);
  }
});

// Boot check: run once on the sample so the page is never empty.
window.addEventListener('DOMContentLoaded', () => {
  stageList.appendChild(makeStage('aggregate'));
  stageList.lastChild.querySelector('input.group').value = 'region';
  stageList.lastChild.querySelector('input.expr').value = 'total = sum(amount)';
  stageList.appendChild(makeStage('sort'));
  stageList.lastChild.querySelector('input').value = 'region';
  document.getElementById('run').click();
});
