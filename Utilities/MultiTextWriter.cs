using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CryptoCandleMetricsProcessor.Utilities
{
    public class MultiTextWriter : TextWriter
    {
        private readonly TextWriter _consoleWriter;
        private readonly TextWriter _fileWriter;

        public MultiTextWriter(TextWriter consoleWriter, TextWriter fileWriter)
        {
            _consoleWriter = consoleWriter;
            _fileWriter = fileWriter;
        }

        public override Encoding Encoding => _consoleWriter.Encoding;

        public override void Write(char value)
        {
            _consoleWriter.Write(value);
            _fileWriter.Write(value);
        }

        public override void Write(string? value)
        {
            _consoleWriter.Write(value);
            _fileWriter.Write(value);
        }

        public override void WriteLine(string? value)
        {
            _consoleWriter.WriteLine(value);
            _fileWriter.WriteLine(value);
        }

        public override void Flush()
        {
            _consoleWriter.Flush();
            _fileWriter.Flush();
        }
    }
}
