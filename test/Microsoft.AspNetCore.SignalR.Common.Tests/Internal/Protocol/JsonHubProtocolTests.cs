using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR.Internal;
using Microsoft.AspNetCore.SignalR.Internal.Protocol;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Xunit;

namespace Microsoft.AspNetCore.SignalR.Common.Tests.Internal.Protocol
{
    public class JsonHubProtocolTests
    {
        public static IEnumerable<object[]> ProtocolTestData => new[]
        {
            new object[] { new InvocationMessage("123", true, "Target", 1, "Foo", 2.0f), true, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":1,\"target\":\"Target\",\"nonBlocking\":true,\"arguments\":[1,\"Foo\",2.0]}" },
            new object[] { new InvocationMessage("123", false, "Target", 1, "Foo", 2.0f), true, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":1,\"target\":\"Target\",\"arguments\":[1,\"Foo\",2.0]}" },
            new object[] { new InvocationMessage("123", false, "Target", true), true, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":1,\"target\":\"Target\",\"arguments\":[true]}" },
            new object[] { new InvocationMessage("123", false, "Target", new object[] { null }), true, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":1,\"target\":\"Target\",\"arguments\":[null]}" },
            new object[] { new InvocationMessage("123", false, "Target", new CustomObject()), false, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":1,\"target\":\"Target\",\"arguments\":[{\"StringProp\":\"SignalR!\",\"DoubleProp\":6.2831853071,\"IntProp\":42,\"DateTimeProp\":\"2017-04-11T00:00:00\"}]}" },
            new object[] { new InvocationMessage("123", false, "Target", new CustomObject()), true, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":1,\"target\":\"Target\",\"arguments\":[{\"stringProp\":\"SignalR!\",\"doubleProp\":6.2831853071,\"intProp\":42,\"dateTimeProp\":\"2017-04-11T00:00:00\"}]}" },
            new object[] { new InvocationMessage("123", false, "Target", new CustomObject()), false, NullValueHandling.Include, "{\"invocationId\":\"123\",\"type\":1,\"target\":\"Target\",\"arguments\":[{\"StringProp\":\"SignalR!\",\"DoubleProp\":6.2831853071,\"IntProp\":42,\"DateTimeProp\":\"2017-04-11T00:00:00\",\"NullProp\":null}]}" },
            new object[] { new InvocationMessage("123", false, "Target", new CustomObject()), true, NullValueHandling.Include, "{\"invocationId\":\"123\",\"type\":1,\"target\":\"Target\",\"arguments\":[{\"stringProp\":\"SignalR!\",\"doubleProp\":6.2831853071,\"intProp\":42,\"dateTimeProp\":\"2017-04-11T00:00:00\",\"nullProp\":null}]}" },

            new object[] { new StreamItemMessage("123", 1), true, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":2,\"result\":1}" },
            new object[] { new StreamItemMessage("123", "Foo"), true, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":2,\"result\":\"Foo\"}" },
            new object[] { new StreamItemMessage("123", 2.0f), true, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":2,\"result\":2.0}" },
            new object[] { new StreamItemMessage("123", true), true, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":2,\"result\":true}" },
            new object[] { new StreamItemMessage("123", null), true, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":2,\"result\":null}" },
            new object[] { new StreamItemMessage("123", new CustomObject()), false, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":2,\"result\":{\"StringProp\":\"SignalR!\",\"DoubleProp\":6.2831853071,\"IntProp\":42,\"DateTimeProp\":\"2017-04-11T00:00:00\"}}" },
            new object[] { new StreamItemMessage("123", new CustomObject()), true, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":2,\"result\":{\"stringProp\":\"SignalR!\",\"doubleProp\":6.2831853071,\"intProp\":42,\"dateTimeProp\":\"2017-04-11T00:00:00\"}}" },
            new object[] { new StreamItemMessage("123", new CustomObject()), false, NullValueHandling.Include, "{\"invocationId\":\"123\",\"type\":2,\"result\":{\"StringProp\":\"SignalR!\",\"DoubleProp\":6.2831853071,\"IntProp\":42,\"DateTimeProp\":\"2017-04-11T00:00:00\",\"NullProp\":null}}" },
            new object[] { new StreamItemMessage("123", new CustomObject()), true, NullValueHandling.Include, "{\"invocationId\":\"123\",\"type\":2,\"result\":{\"stringProp\":\"SignalR!\",\"doubleProp\":6.2831853071,\"intProp\":42,\"dateTimeProp\":\"2017-04-11T00:00:00\",\"nullProp\":null}}" },

            new object[] { CompletionMessage.WithResult("123", 1), true, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":3,\"result\":1}" },
            new object[] { CompletionMessage.WithResult("123", "Foo"), true, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":3,\"result\":\"Foo\"}" },
            new object[] { CompletionMessage.WithResult("123", 2.0f), true, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":3,\"result\":2.0}" },
            new object[] { CompletionMessage.WithResult("123", true), true, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":3,\"result\":true}" },
            new object[] { CompletionMessage.WithResult("123", null), true, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":3,\"result\":null}" },
            new object[] { CompletionMessage.WithError("123", "Whoops!"), false, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":3,\"error\":\"Whoops!\"}" },
            new object[] { CompletionMessage.WithResult("123", new CustomObject()), false, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":3,\"result\":{\"StringProp\":\"SignalR!\",\"DoubleProp\":6.2831853071,\"IntProp\":42,\"DateTimeProp\":\"2017-04-11T00:00:00\"}}" },
            new object[] { CompletionMessage.WithResult("123", new CustomObject()), true, NullValueHandling.Ignore, "{\"invocationId\":\"123\",\"type\":3,\"result\":{\"stringProp\":\"SignalR!\",\"doubleProp\":6.2831853071,\"intProp\":42,\"dateTimeProp\":\"2017-04-11T00:00:00\"}}" },
            new object[] { CompletionMessage.WithResult("123", new CustomObject()), false, NullValueHandling.Include, "{\"invocationId\":\"123\",\"type\":3,\"result\":{\"StringProp\":\"SignalR!\",\"DoubleProp\":6.2831853071,\"IntProp\":42,\"DateTimeProp\":\"2017-04-11T00:00:00\",\"NullProp\":null}}" },
            new object[] { CompletionMessage.WithResult("123", new CustomObject()), true, NullValueHandling.Include, "{\"invocationId\":\"123\",\"type\":3,\"result\":{\"stringProp\":\"SignalR!\",\"doubleProp\":6.2831853071,\"intProp\":42,\"dateTimeProp\":\"2017-04-11T00:00:00\",\"nullProp\":null}}" },
        };

        [Theory]
        [MemberData(nameof(ProtocolTestData))]
        public async Task WriteMessage(HubMessage message, bool camelCase, NullValueHandling nullValueHandling, string expectedOutput)
        {
            var jsonSerializer = new JsonSerializer()
            {
                NullValueHandling = nullValueHandling,
                ContractResolver = camelCase ? new CamelCasePropertyNamesContractResolver() : new DefaultContractResolver()
            };

            var protocol = new JsonHubProtocol(jsonSerializer);
            var encoded = await protocol.WriteToArrayAsync(message);
            var json = Encoding.UTF8.GetString(encoded);

            Assert.Equal(expectedOutput, json);
        }

        [Theory]
        [MemberData(nameof(ProtocolTestData))]
        public void ParseMessage(HubMessage expectedMessage, bool camelCase, NullValueHandling nullValueHandling, string input)
        {
            var jsonSerializer = new JsonSerializer()
            {
                NullValueHandling = nullValueHandling,
                ContractResolver = camelCase ? new CamelCasePropertyNamesContractResolver() : new DefaultContractResolver()
            };

            var binder = new TestBinder(expectedMessage);
            var protocol = new JsonHubProtocol(jsonSerializer);
            var message = protocol.ParseMessage(Encoding.UTF8.GetBytes(input), binder);

            Assert.Equal(expectedMessage, message, TestEqualityComparer.Instance);
        }

        private class CustomObject : IEquatable<CustomObject>
        {
            // Not intended to be a full set of things, just a smattering of sample serializations
            public string StringProp => "SignalR!";

            public double DoubleProp => 6.2831853071;

            public int IntProp => 42;

            public DateTime DateTimeProp => new DateTime(2017, 4, 11);

            public object NullProp => null;

            public override bool Equals(object obj)
            {
                return obj is CustomObject o && Equals(o);
            }

            public override int GetHashCode()
            {
                // This is never used in a hash table
                return 0;
            }

            public bool Equals(CustomObject right)
            {
                return string.Equals(StringProp, right.StringProp, StringComparison.Ordinal) &&
                    DoubleProp == right.DoubleProp &&
                    IntProp == right.IntProp &&
                    DateTime.Equals(DateTimeProp, right.DateTimeProp) &&
                    NullProp == right.NullProp;
            }
        }

        // Binder that works based on the expected message argument/result types :)
        private class TestBinder : IInvocationBinder
        {
            private HubMessage _expectedMessage;

            public TestBinder(HubMessage expectedMessage)
            {
                _expectedMessage = expectedMessage;
            }

            public Type[] GetParameterTypes(string methodName)
            {
                if (_expectedMessage is InvocationMessage m)
                {
                    return m.Arguments.Select(a => a == null ? typeof(object) : a.GetType()).ToArray();
                }
                throw new InvalidOperationException("Unexpected binder call");
            }

            public Type GetReturnType(string invocationId)
            {
                switch (_expectedMessage)
                {
                    case CompletionMessage m:
                        return m.Result?.GetType() ?? typeof(object);
                    case StreamItemMessage m:
                        return m.Item?.GetType() ?? typeof(object);
                    default:
                        throw new InvalidOperationException("Unexpected binder call");
                }
            }
        }

        private class TestEqualityComparer : IEqualityComparer<HubMessage>
        {
            public static readonly TestEqualityComparer Instance = new TestEqualityComparer();

            private TestEqualityComparer() { }

            public bool Equals(HubMessage x, HubMessage y)
            {
                if (!string.Equals(x.InvocationId, y.InvocationId, StringComparison.Ordinal))
                {
                    return false;
                }

                return InvocationMessagesEqual(x, y) || StreamItemMessagesEqual(x, y) || CompletionMessagesEqual(x, y);
            }

            private bool CompletionMessagesEqual(HubMessage x, HubMessage y)
            {
                return x is CompletionMessage left && y is CompletionMessage right &&
                    string.Equals(left.Error, right.Error, StringComparison.Ordinal) &&
                    Equals(left.Result, right.Result) &&
                    left.HasResult == right.HasResult;
            }

            private bool StreamItemMessagesEqual(HubMessage x, HubMessage y)
            {
                return x is StreamItemMessage left && y is StreamItemMessage right &&
                    Equals(left.Item, right.Item);
            }

            private bool InvocationMessagesEqual(HubMessage x, HubMessage y)
            {
                return x is InvocationMessage left && y is InvocationMessage right &&
                    string.Equals(left.Target, right.Target, StringComparison.Ordinal) &&
                    Enumerable.SequenceEqual(left.Arguments, right.Arguments) &&
                    left.NonBlocking == right.NonBlocking;
            }

            public int GetHashCode(HubMessage obj)
            {
                // We never use these in a hash-table
                return 0;
            }
        }
    }
}
