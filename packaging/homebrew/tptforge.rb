# Homebrew formula template. Copy into a tap (e.g. tpt-solutions/homebrew-tap,
# Formula/tptforge.rb) after each release and replace every REPLACE_* sha256
# with the matching line from the release's SHA256SUMS file.
class Tptforge < Formula
  desc "TOML-driven streaming ETL pipelines (tpt-streamforge CLI)"
  homepage "https://github.com/tpt-solutions/tpt-streamforge"
  version "0.1.0"
  license any_of: ["MIT", "Apache-2.0"]

  on_macos do
    on_arm do
      url "https://github.com/tpt-solutions/tpt-streamforge/releases/download/v0.1.0/tptforge-v0.1.0-aarch64-apple-darwin.tar.gz"
      sha256 "REPLACE_WITH_SHA256_AARCH64_APPLE_DARWIN"
    end
    on_intel do
      url "https://github.com/tpt-solutions/tpt-streamforge/releases/download/v0.1.0/tptforge-v0.1.0-x86_64-apple-darwin.tar.gz"
      sha256 "REPLACE_WITH_SHA256_X86_64_APPLE_DARWIN"
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/tpt-solutions/tpt-streamforge/releases/download/v0.1.0/tptforge-v0.1.0-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "REPLACE_WITH_SHA256_X86_64_UNKNOWN_LINUX_GNU"
    end
  end

  def install
    bin.install Dir["tptforge-*/tptforge"]
  end

  test do
    assert_match "tptforge", shell_output("#{bin}/tptforge --help")
  end
end
