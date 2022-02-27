using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.Collections;

/// <summary> MODBUS TCP Driver Class. </summary>
namespace MODBUS_TCP_Client
{
    /// <summary> Data Type Enumeration </summary>
    /// <remarks>This Enum defines tags for OnUpdate event.</remarks>
    public enum DataTypeEnum : byte
    {
        /// <remarks> Tag for Discrete Input </remarks>
        dtDiscreteInput = 1,
        /// <remarks> Tag for Coil </remarks>
        dtCoil,
        /// <remarks> Tag for Input Register </remarks>
        dtInputRegister,
        /// <remarks> Tag for Holding Register </remarks>
        dtHoldingRegister
    }

    /// <summary> MODBUS TCP Client Driver Class.</summary>
    /// <remarks> 
    /// The class supports the following commands:
    ///<list type="bullet">
    ///<item>Read Discrete Inputs</item>
    ///<item>Read Coils</item>
    ///<item>Write Single Coil</item>
    ///<item>Write Multiple Coils</item>
    ///<item>Read Input Registers</item>
    ///<item>Read Holding Registers</item>
    ///<item>Write Single Register</item>
    ///<item>Write Multiple Registers</item>
    ///<item>Mask Write Register</item>
    ///</list>
    ///</remarks> 
    public class Client
    {
        // --------------- Constants ---------------

        // Function Codes
        private const byte fcReadDiscreteInputs = 2;
        private const byte fcReadCoils = 1;
        private const byte fcWriteSingleCoil = 5;
        private const byte fcWriteMultipleCoils = 15;
        private const byte fcReadInputRegisters = 4;
        private const byte fcReadHoldingRegisters = 3;
        private const byte fcWriteSingleRegister = 6;
        private const byte fcWriteMultipleRegisters = 16;
        private const byte fcMaskWriteRegister = 22;
        private const byte fcReadWriteMultipleRegisters = 23;

        // Exception Codes
        private const byte exIllegalFunction = 1;
        private const byte exIllegalDataAddress = 2;
        private const byte exIllegalDataValue = 3;
        private const byte exServerDeviceFailure = 4;

        // --------------- Fields ---------------

        //Objects
        private TcpClient tcpClient;
        private NetworkStream netStream;
        private Random random;

        // Variables
        private ushort startAddress;
        private ushort length;
        private byte[] pdu;
        private byte[] adu;
        private byte[] receivedPDU;

        //Properties
        private bool connected = false;
        private ushort timeout = 500;
        public bool echo = false;

        // --------------- Delegates and Events ---------------

        public delegate void ConnectedDelegate();
        public delegate void UpdateDelegate(DataTypeEnum dType, bool[] Bits, ushort[] Registers, ushort startAddress);
        public delegate void ExceptionDelegate(string exceptionMessage);

        /// <summary> This event is called when the connection established. </summary>
        public event ConnectedDelegate OnConnected;
        /// <summary> This event is called when valid data arrives from server. </summary>
        public event UpdateDelegate OnUpdate;
        /// <summary> This event is called when an exception arises. </summary>
        public event ExceptionDelegate OnException;

        // --------------- Constructor and Destructor ---------------

        /// <summary> Constructor </summary>
        public Client()
        {
            tcpClient = new TcpClient();
            random = new Random();
        }

        /// <summary> Destructor </summary>
        ~Client()
        {
            this.Disconnect();
        }

        // --------------- Properties ---------------

        /// <summary> Responce timeout (ms). </summary>
        /// <remarks> If the server does not answer whitin this time an exception is called. </remarks>
        /// <value> Default: 500ms </value>
        public ushort Timeout
        {
            get
            {
                return timeout;
            }
            set
            {
                timeout = value;
            }
        }

        /// <summary> Write echo. </summary>
        /// <remarks>
        /// Forces to make a read function after writing on Coils and Registers. 
        /// Valid only for following commands:
        ///<list type="bullet">
        ///<item>Write Multiple Coils</item>
        ///<item>Write Multiple Registers</item>
        ///<item>Mask Write Register</item>
        ///</list>
        /// </remarks>
        /// <value> Default: False </value>
        public bool Echo
        {
            get
            {
                return echo;
            }
            set
            {
                echo = value;
            }
        }

        /// <summary> Connection Status </summary>
        /// <remarks> Shows the status of the connection. </remarks>
        public bool Connected
        {
            get { return connected; }
        }

        // --------------- Methods ---------------

        /// <summary> Start connection to server. </summary>
        /// <param name="ip">IP address of server </param>
        /// <param name="port">Port number of server</param>
        public void Connect(string ip = "172.26.112.1", ushort port = 502)
        {
            try
            {
                IPAddress ipAddress = IPAddress.Parse(ip);

                tcpClient.Connect(ipAddress, port);

                if (tcpClient.Connected)
                {
                    connected = true;

                    tcpClient.SendBufferSize = 1024;
                    tcpClient.ReceiveBufferSize = 1024;
                    tcpClient.SendTimeout = timeout;
                    tcpClient.ReceiveTimeout = timeout;
                    tcpClient.NoDelay = true;

                    netStream = tcpClient.GetStream();

                    OnConnected?.Invoke();
                }
            }
            catch (Exception ex)
            {
                OnException?.Invoke($"Client: {ex.Message}");
            }
        }

        /// <summary>Stop connection to server.</summary>
        public void Disconnect()
        {
            if (tcpClient != null)
            {
                if (tcpClient.Connected)
                {
                    tcpClient.Close();
                }

                connected = false;

                tcpClient = null;
                netStream = null;
                random = null;
            }
        }

        /// <summary>Read from Discrete Inputs of server. </summary>
        /// <param name="startAddress">Start Address.</param>
        /// <param name="length">Length of data.</param>
        public void ReadDiscreteInputs(ushort startAddress, ushort length)
        {
            if (CheckAddress(startAddress, length, 65535, 2000))
            {
                pdu = new byte[5];

                byte[] address = BitConverter.GetBytes(startAddress);
                byte[] size = BitConverter.GetBytes(length);

                pdu[0] = fcReadDiscreteInputs;
                pdu[1] = address[1];
                pdu[2] = address[0];
                pdu[3] = size[1];
                pdu[4] = size[0];

                WriteToStream();
            }
        }

        /// <summary>Read from Coils of server. </summary>
        /// <param name="startAddress">Start Address.</param>
        /// <param name="length">Length of data.</param>
        public void ReadCoils(ushort startAddress, ushort length)
        {
            if (CheckAddress(startAddress, length, 65535, 2000))
            {
                pdu = new byte[5];

                byte[] address = BitConverter.GetBytes(startAddress);
                byte[] size = BitConverter.GetBytes(length);

                pdu[0] = fcReadCoils;
                pdu[1] = address[1];
                pdu[2] = address[0];
                pdu[3] = size[1];
                pdu[4] = size[0];

                WriteToStream();
            }
        }

        /// <summary>Write to a Coil of server. </summary>
        /// <param name="startAddress">Start Address.</param>
        /// <param name="data">Coil state.</param>
        public void WriteSingleCoil(ushort startAddress, bool data)
        {
            if (CheckAddress(startAddress, 1, 65535, 1))
            {
                pdu = new byte[5];

                byte[] address = BitConverter.GetBytes(startAddress);

                pdu[0] = fcWriteSingleCoil;
                pdu[1] = address[1];
                pdu[2] = address[0];
                pdu[3] = (byte)(Convert.ToByte(data) * 255);
                pdu[4] = 0;

                WriteToStream();
            }
        }

        /// <summary>Write to Coils of server. </summary>
        /// <param name="startAddress">Start Address.</param>
        /// <param name="data">Coils state.</param>
        public void WriteMultipleCoils(ushort startAddress, bool[] data)
        {
            ushort length = (ushort)data.Length;
            if (CheckAddress(startAddress, length, 65535, 1968))
            {
                byte byteCount = (byte)Math.Ceiling(((double)length / 8));

                pdu = new byte[byteCount + 6];

                byte[] address = BitConverter.GetBytes(startAddress);
                byte[] size = BitConverter.GetBytes(length);
                bool[] sliceValue;
                BitArray bits;

                pdu[0] = fcWriteMultipleCoils;
                pdu[1] = address[1];
                pdu[2] = address[0];
                pdu[3] = size[1];
                pdu[4] = size[0];
                pdu[5] = byteCount;
                for (int i = 0; i < byteCount; i++)
                {
                    sliceValue = new bool[8];
                    Array.Copy(data, i * 8, sliceValue, 0, Math.Min(length - (i * 8), 8));
                    bits = new BitArray(sliceValue);
                    bits.CopyTo(pdu, 6 + i);
                }

                WriteToStream();
            }
        }

        /// <summary>Read from Input Registers of server. </summary>
        /// <param name="startAddress">Start Address.</param>
        /// <param name="data">Length of data.</param>
        public void ReadInputRegisters(ushort startAddress, ushort length)
        {
            if (CheckAddress(startAddress, length, 65535, 125))
            {
                pdu = new byte[5];

                byte[] address = BitConverter.GetBytes(startAddress);
                byte[] size = BitConverter.GetBytes(length);

                pdu[0] = fcReadInputRegisters;
                pdu[1] = address[1];
                pdu[2] = address[0];
                pdu[3] = size[1];
                pdu[4] = size[0];

                WriteToStream();
            }
        }

        /// <summary>Read from Holding Registers of server. </summary>
        /// <param name="startAddress">Start Address.</param>
        /// <param name="data">Length of data.</param>
        public void ReadHoldingRegisters(ushort startAddress, ushort length)
        {
            if (CheckAddress(startAddress, length, 65535, 125))
            {
                pdu = new byte[5];

                byte[] address = BitConverter.GetBytes(startAddress);
                byte[] size = BitConverter.GetBytes(length);

                pdu[0] = fcReadHoldingRegisters;
                pdu[1] = address[1];
                pdu[2] = address[0];
                pdu[3] = size[1];
                pdu[4] = size[0];

                WriteToStream();
            }
        }

        /// <summary>Write to a Holding Register of server. </summary>
        /// <param name="startAddress">Start Address.</param>
        /// <param name="data">Register value.</param>
        public void WriteSingleRegister(ushort startAddress, ushort data)
        {
            if (CheckAddress(startAddress, 1, 65535, 1))
            {
                pdu = new byte[5];

                byte[] address = BitConverter.GetBytes(startAddress);
                byte[] value = BitConverter.GetBytes(data);

                pdu[0] = fcWriteSingleRegister;
                pdu[1] = address[1];
                pdu[2] = address[0];
                pdu[3] = value[1];
                pdu[4] = value[0];

                WriteToStream();
            }
        }

        /// <summary>Write to Holding Registers of server. </summary>
        /// <param name="startAddress">Start Address.</param>
        /// <param name="data">Register value.</param>
        public void WriteMultipleRegisters(ushort startAddress, ushort[] data)
        {
            ushort length = (ushort)data.Length;
            if (CheckAddress(startAddress, length, 65535, 123))
            {
                byte byteCount = (byte)(length * 2);

                pdu = new byte[byteCount + 6];

                byte[] address = BitConverter.GetBytes(startAddress);
                byte[] size = BitConverter.GetBytes(length);
                byte[] value;

                pdu[0] = fcWriteMultipleRegisters;
                pdu[1] = address[1];
                pdu[2] = address[0];
                pdu[3] = size[1];
                pdu[4] = size[0];
                pdu[5] = byteCount;
                for (int i = 0; i < length; i++)
                {
                    value = BitConverter.GetBytes(data[i]);

                    pdu[6 + i * 2] = value[1];
                    pdu[7 + i * 2] = value[0];
                }

                WriteToStream();
            }
        }

        /// <summary>Mask a Holding Register of server.</summary>
        /// <param name="startAddress">Start Address.</param>
        /// <param name="maskAND">AND Mask</param>
        /// <param name="maskOR">OR Mask</param>
        public void MaskWriteRegister(ushort startAddress, ushort maskAND, ushort maskOR)
        {
            if (CheckAddress(startAddress, 1, 65535, 1))
            {
                pdu = new byte[7];

                byte[] address = BitConverter.GetBytes(startAddress);
                byte[] maskANDValue = BitConverter.GetBytes(maskAND);
                byte[] maskORValue = BitConverter.GetBytes(maskOR);

                pdu[0] = fcMaskWriteRegister;
                pdu[1] = address[1];
                pdu[2] = address[0];
                pdu[3] = maskANDValue[1];
                pdu[4] = maskANDValue[0];
                pdu[5] = maskORValue[1];
                pdu[6] = maskORValue[0];

                WriteToStream();
            }
        }

        // --------------- Functions ---------------

        private bool CheckAddress(ushort startAddress, ushort length, ushort maxStartAddress, ushort maxLength)
        {
            if (startAddress <= maxStartAddress)
            {
                if ((length <= maxLength) && (length >= 1))
                {
                    if (startAddress + length <= maxStartAddress)
                    {
                        this.startAddress = startAddress;
                        this.length = length;

                        return true;
                    }
                    else
                    {
                        OnException?.Invoke($"Client: End Address is out of range!");
                    }
                }
                else
                {
                    OnException?.Invoke($"Client: Address Length is out of range!");
                }
            }
            else
            {
                OnException?.Invoke($"Client: Start Address is out of range!");
            }

            return false;
        }

        private void CreateADU()
        {
            byte[] mbap = CreateMBAP();

            ushort lengthPDU = (ushort)pdu.Length;

            adu = new byte[lengthPDU + 7];
            mbap.CopyTo(adu, 0);
            pdu.CopyTo(adu, 7);
        }

        private byte[] CreateMBAP()
        {
            byte[] mbap = new byte[7];

            ushort transactionID = (ushort)random.Next(UInt16.MaxValue + 1);
            ushort lengthPDU = (ushort)pdu.Length;

            byte[] id = BitConverter.GetBytes(transactionID);
            byte[] size = BitConverter.GetBytes(lengthPDU + 1);

            mbap[0] = id[1];
            mbap[1] = id[0];
            mbap[2] = 0;
            mbap[3] = 0;
            mbap[4] = size[1];
            mbap[5] = size[0];
            mbap[6] = 255;

            return mbap;
        }

        private void WriteToStream()
        {
            CreateADU();

            netStream.Write(adu, 0, adu.Length);

            byte[] buffer = new byte[1024];

            try
            {
                netStream.Read(buffer, 0, buffer.Length);

                if (Validate(buffer))
                {
                    Decode();
                }
            }
            catch (Exception ex)
            {
                OnException?.Invoke($"Client: {ex.Message}");
            }
        }

        private bool Validate(byte[] buffer)
        {
            if ((buffer[0] == adu[0]) && (buffer[1] == adu[1]))
            {
                if ((buffer[2] == adu[2]) && (buffer[3] == adu[3]))
                {
                    if (buffer[7] == adu[7])
                    {
                        ushort size = SwapBytes16(BitConverter.ToUInt16(buffer, 4));

                        receivedPDU = new byte[size - 1];
                        Array.Copy(buffer, 7, receivedPDU, 0, size - 1);

                        return true;
                    }
                    else if (buffer[7] - 128 == adu[7])
                    {
                        byte exceptionCode = buffer[8];
                        string exceptionMessage;

                        switch (exceptionCode)
                        {
                            case exIllegalFunction:
                                byte functionCode = (byte)(buffer[7] - 128);
                                exceptionMessage = $"Server: {functionCode} Function Code is not supported!";
                                break;
                            case exIllegalDataAddress:
                                exceptionMessage = "Server: Address is not available!";
                                break;
                            case exIllegalDataValue:
                                exceptionMessage = "Server: Data is not supported!";
                                break;
                            case exServerDeviceFailure:
                                exceptionMessage = "Server: Responce failed!";
                                break;
                            default:
                                exceptionMessage = "Server: Unknowen exception!";
                                break;
                        }
                        OnException?.Invoke(exceptionMessage);
                    }
                    else
                    {
                        OnException?.Invoke($"Client: Function Code mismatch!");
                    }
                }
                else
                {
                    OnException?.Invoke($"Client: Protocol is not Modbus!");
                }
            }
            else
            {
                OnException?.Invoke($"Client: Transaction ID mismatch!");
            }

            return false;
        }

        private void Decode()
        {
            byte functionCode = receivedPDU[0];
            DataTypeEnum dType = DataTypeEnum.dtDiscreteInput;

            switch (functionCode)
            {
                case fcReadDiscreteInputs:
                    dType = DataTypeEnum.dtDiscreteInput;
                    break;

                case fcReadCoils:
                case fcWriteSingleCoil:
                case fcWriteMultipleCoils:
                    dType = DataTypeEnum.dtCoil;
                    break;

                case fcReadInputRegisters:
                    dType = DataTypeEnum.dtInputRegister;
                    break;

                case fcReadHoldingRegisters:
                case fcWriteSingleRegister:
                case fcWriteMultipleRegisters:
                case fcMaskWriteRegister:
                    dType = DataTypeEnum.dtHoldingRegister;
                    break;
            }

            ushort startAddress;
            ushort length;
            byte byteCount;
            byte[] data;
            bool[] bitsOut;
            ushort[] registersOut;

            switch (functionCode)
            {
                case fcReadDiscreteInputs:
                case fcReadCoils:
                    byteCount = receivedPDU[1];
                    data = new byte[byteCount];

                    Array.Copy(receivedPDU, 2, data, 0, byteCount);

                    bitsOut = new bool[this.length];

                    BitArray bits = new BitArray(data);

                    for (int i = 0; i < this.length; i++)
                    {
                        bitsOut[i] = bits[i];
                    }

                    OnUpdate?.Invoke(dType, bitsOut, null, this.startAddress);

                    break;

                case fcWriteSingleCoil:
                    startAddress = SwapBytes16(BitConverter.ToUInt16(receivedPDU, 1));

                    bitsOut = new bool[1];
                    bitsOut[0] = Convert.ToBoolean(receivedPDU[3]);

                    OnUpdate?.Invoke(dType, bitsOut, null, startAddress);

                    break;

                case fcWriteMultipleCoils:
                    startAddress = SwapBytes16(BitConverter.ToUInt16(receivedPDU, 1));
                    length = SwapBytes16(BitConverter.ToUInt16(receivedPDU, 3));

                    if (Echo)
                    {
                        ReadCoils(startAddress, length);
                    }
                    else
                    {
                        OnUpdate?.Invoke(dType, null, null, startAddress);
                    }

                    break;

                case fcReadInputRegisters:
                case fcReadHoldingRegisters:
                    byteCount = receivedPDU[1];

                    data = new byte[byteCount];

                    Array.Copy(receivedPDU, 2, data, 0, byteCount);

                    registersOut = new ushort[byteCount / 2];

                    for (int i = 0; i < byteCount / 2; i++)
                    {
                        registersOut[i] = SwapBytes16(BitConverter.ToUInt16(data, 2 * i));
                    }

                    OnUpdate?.Invoke(dType, null, registersOut, this.startAddress);

                    break;

                case fcWriteSingleRegister:
                    startAddress = SwapBytes16(BitConverter.ToUInt16(receivedPDU, 1));

                    registersOut = new ushort[1];
                    registersOut[0] = SwapBytes16(BitConverter.ToUInt16(receivedPDU, 3));

                    OnUpdate?.Invoke(dType, null, registersOut, startAddress);

                    break;

                case fcWriteMultipleRegisters:
                    startAddress = SwapBytes16(BitConverter.ToUInt16(receivedPDU, 1));
                    length = SwapBytes16(BitConverter.ToUInt16(receivedPDU, 3));


                    if (Echo)
                    {
                        ReadHoldingRegisters(startAddress, length);
                    }
                    else
                    {
                        OnUpdate?.Invoke(dType, null, null, startAddress);
                    }

                    break;

                case fcMaskWriteRegister:
                    startAddress = SwapBytes16(BitConverter.ToUInt16(receivedPDU, 1));


                    if (Echo)
                    {
                        ReadHoldingRegisters(startAddress, 1);
                    }
                    else
                    {
                        OnUpdate?.Invoke(dType, null, null, startAddress);
                    }

                    break;
            }
        }

        internal static ushort SwapBytes16(UInt16 inValue)
        {
            return (ushort)(((inValue & 0xff00) >> 8) | ((inValue & 0x00ff) << 8));
        }

        private void PrintBytes(byte[] data)
        {
            foreach (byte datum in data)
            {
                Console.Write(datum);
                Console.Write(" ");
            }
            Console.WriteLine();
        }
    }
}
