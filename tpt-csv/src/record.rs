/// One CSV record: a sequence of UTF-8 fields.
///
/// Reused across reads (via [`clear`](StringRecord::clear)) to avoid
/// per-record allocation when reading many rows.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StringRecord {
    /// Field bytes, concatenated.
    data: String,
    /// Byte offsets into `data` marking the end of each field; field `i`
    /// spans `ends[i-1]..ends[i]` (or `0..ends[0]` for the first field).
    ends: Vec<usize>,
}

impl StringRecord {
    pub fn new() -> Self {
        StringRecord::default()
    }

    /// Append one field.
    pub fn push_field(&mut self, field: &str) {
        self.data.push_str(field);
        self.ends.push(self.data.len());
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.ends.clear();
    }

    pub fn len(&self) -> usize {
        self.ends.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&str> {
        let start = if index == 0 { 0 } else { self.ends[index - 1] };
        let end = *self.ends.get(index)?;
        Some(&self.data[start..end])
    }

    pub fn iter(&self) -> Iter<'_> {
        Iter {
            record: self,
            index: 0,
        }
    }
}

pub struct Iter<'a> {
    record: &'a StringRecord,
    index: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        let field = self.record.get(self.index)?;
        self.index += 1;
        Some(field)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.record.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

impl<'a> IntoIterator for &'a StringRecord {
    type Item = &'a str;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_and_iterate_fields() {
        let mut r = StringRecord::new();
        r.push_field("a");
        r.push_field("");
        r.push_field("bc");
        assert_eq!(r.len(), 3);
        assert_eq!(r.iter().collect::<Vec<_>>(), vec!["a", "", "bc"]);
        assert_eq!(r.get(2), Some("bc"));
        assert_eq!(r.get(3), None);
    }

    #[test]
    fn clear_resets_state() {
        let mut r = StringRecord::new();
        r.push_field("x");
        r.clear();
        assert!(r.is_empty());
        assert_eq!(r.get(0), None);
    }
}
