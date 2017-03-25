// <copyright company="James Hough">
//   Copyright (c) James Hough. Licensed under MIT License - refer to LICENSE file
// </copyright>
namespace Trifling.Caching.AspNet
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;

    using Microsoft.Extensions.Caching.Memory;

    using Trifling.Caching.Interfaces;
    using Trifling.Comparison;

    /// <summary>
    /// An implementation of <see cref="ICacheEngine"/> for caching on a ASP.Net cache engine. 
    /// </summary>
    public class AspNetCacheEngine : ICacheEngine
    {
        /// <summary>
        /// The instance of the cache to access cached items.
        /// </summary>
        private IMemoryCache _memoryCache;

        /// <summary>
        /// All open locks for cache entries.
        /// </summary>
        private ConcurrentDictionary<string, object> _activeLocks;

        /// <summary>
        /// The age of all open locks (for cleaning up older entries).
        /// </summary>
        private ConcurrentDictionary<string, DateTime> _lockObjectAge;

        /// <summary>
        /// Initialises a new instance of the <see cref="AspNetCacheEngine"/> class with the given
        /// ASP.Net caching provider.
        /// </summary>
        /// <param name="memoryCache">An instance of <see cref="IMemoryCache"/> to access cached items.</param>
        public AspNetCacheEngine(IMemoryCache memoryCache)
        {
            this._memoryCache = memoryCache;
            this._activeLocks = new ConcurrentDictionary<string, object>();
            this._lockObjectAge = new ConcurrentDictionary<string, DateTime>();
        }

        /// <summary>
        /// Initialises the connection to ASP.Net cache engine with the given configuration.
        /// </summary>
        /// <param name="cacheEngineConfiguration">The configuration options for connecting to ASP.Net cache engine.</param>
        public void Initialize(CacheEngineConfiguration cacheEngineConfiguration)
        {
            // there isn't anything to do for this cache engine.
        }

        /// <summary>
        /// Deletes the cache entry with the matching unique key. If the entry was found and successfully removed
        /// then this will return true.  Otherwise this will return false.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key identifying the cache entry to delete.</param>
        /// <returns>Returns true if the entry was found and successfully removed from the cache. Otherwise false.</returns>
        public bool Remove(string cacheEntryKey)
        {
            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    object objectValue;

                    if (this._memoryCache.TryGetValue(cacheEntryKey, out objectValue))
                    {
                        this._memoryCache.Remove(cacheEntryKey);
                        return true;
                    }

                    return false;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Checks if any cached value exists with the specified <paramref name="cacheEntryKey"/>.
        /// </summary>
        /// <param name="cacheEntryKey">The unique identifier of the cache entry to seek in cache.</param>
        /// <returns>If the cache entry was found, then returns true. Otherwise returns false.</returns>
        public bool Exists(string cacheEntryKey)
        {
            object objectValue;
            return this._memoryCache.TryGetValue(cacheEntryKey, out objectValue);
        }

        #region Single value caching

        /// <summary>
        /// Stores the given value in the ASP.Net Cache engine.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the entry to create or overwrite in the cache.</param>
        /// <param name="value">The data to store in the cache.</param>
        /// <param name="expiry">The time period that the data will be valid.</param>
        /// <returns>Returns true if the value was successfully cached. Otherwise false.</returns>
        public bool Cache(string cacheEntryKey, byte[] value, TimeSpan expiry)
        {
            this._memoryCache.Set(cacheEntryKey, value, expiry);
            return true;
        }

        /// <summary>
        /// Fetches a stored value from the cache. If the key was found then the value is returned. If not 
        /// found then a null is returned.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry to attempt to retrieve.</param>
        /// <returns>Returns the located value from the cache if the key was found. Otherwise null.</returns>
        public byte[] Retrieve(string cacheEntryKey)
        {
            object objectValue;
            if (!this._memoryCache.TryGetValue(cacheEntryKey, out objectValue))
            {
                return null;
            }

            return (byte[])objectValue;
        }

        #endregion Single value caching

        #region Set caching

        /// <summary>
        /// Caches the given enumeration of <paramref name="setItems"/> as a set in the cache.
        /// </summary>
        /// <remarks>
        /// Items of a set are not guaranteed to retain ordering when retrieved from cache. The
        /// implementation of <see cref="RetrieveSet{T}"/> returns a sorted set even if the input
        /// was not sorted.
        /// </remarks>
        /// <typeparam name="T">The type of object being cached. All items of the set must be of this type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry to create.</param>
        /// <param name="setItems">The individual items to store as a set.</param>
        /// <param name="expiry">The time period that the data will be valid.</param>
        /// <returns>Returns true if the set was successfully created with all <paramref name="setItems"/> values cached.</returns>
        public bool CacheAsSet<T>(string cacheEntryKey, IEnumerable<T> setItems, TimeSpan expiry)
            where T : IConvertible
        {
            object existing;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var existedPreviously = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    SortedSet<string> newSet;

                    // values must be string convertable
                    var stringValues = setItems
                        .Select(item => ValueToString(item))
                        .ToArray();

                    if (existedPreviously && !(existing is SortedSet<string>))
                    {
                        // if the cache key exists and it's not a set of strings then it cannot be updated
                        return false;
                    }
                    else if (existedPreviously)
                    {
                        // we can append to the existing set.
                        newSet = existing as SortedSet<string>;

                        foreach (var item in stringValues)
                        {
                            newSet.Add(item);
                        }
                    }
                    else
                    {
                        // create a new concurrent set for the new cached value.
                        newSet = new SortedSet<string>(stringValues);
                    }

                    this._memoryCache.Set(cacheEntryKey, newSet, expiry);
                    return true;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Caches the given enumeration of <paramref name="setItems"/> byte arrays as a set in the cache.
        /// </summary>
        /// <remarks>
        /// Items of a set are not guaranteed to retain ordering when retrieved from cache. The
        /// implementation of <see cref="RetrieveSet"/> returns a sorted set even if the input
        /// was not sorted.
        /// </remarks>
        /// <param name="cacheEntryKey">The unique key of the cache entry to create.</param>
        /// <param name="setItems">The individual items to store as a set.</param>
        /// <param name="expiry">The time period that the data will be valid.</param>
        /// <returns>Returns true if the set was successfully created with all <paramref name="setItems"/> values cached.</returns>
        public bool CacheAsSet(string cacheEntryKey, IEnumerable<byte[]> setItems, TimeSpan expiry)
        {
            object existing;

            try
            { 
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var existedPreviously = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    SortedSet<byte[]> newSet;

                    if (existedPreviously && !(existing is SortedSet<byte[]>))
                    {
                        // if the cache key exists and it's not a set of strings then it cannot be updated
                        return false;
                    }
                    else if (existedPreviously)
                    {
                        // we can append to the existing set.
                        newSet = existing as SortedSet<byte[]>;

                        foreach (var item in setItems)
                        {
                            newSet.Add(item);
                        }
                    }
                    else
                    {
                        // create a new concurrent set for the new cached value.
                        newSet = new SortedSet<byte[]>(setItems, ByteArrayComparer.Default);
                    }

                    this._memoryCache.Set(cacheEntryKey, newSet, expiry);
                    return true;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Adds a single new entry into an existing cached set.
        /// </summary>
        /// <typeparam name="T">The type of object being cached. All existing items of the set must be of this type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry to locate and add to.</param>
        /// <param name="value">The new individual item to store in the existing set.</param>
        /// <returns>Returns false if the set doesn't exist as a cache entry or if the <paramref name="value"/> could not be added to the cached set. Otherwise true.</returns>
        public bool AddToSet<T>(string cacheEntryKey, T value)
            where T : IConvertible
        {
            object existing;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    // prevent a new set from spawning if it doesn't exist already.
                    if (!cacheKeyExists || !(existing is SortedSet<string>))
                    {
                        return false;
                    }

                    // we must append to the existing set.
                    var newSet = existing as SortedSet<string>;

                    // values must be string convertable
                    var givenValue = ValueToString(value);
                    if (newSet.Contains(givenValue))
                    {
                        return false;
                    }

                    newSet.Add(givenValue);
                    return true;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Adds a single new byte array entry into an existing cached set.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry to locate and add to.</param>
        /// <param name="value">The new individual item to store in the existing set.</param>
        /// <returns>Returns false if the set doesn't exist as a cache entry or if the <paramref name="value"/> could not be added to the cached set. Otherwise true.</returns>
        public bool AddToSet(string cacheEntryKey, byte[] value)
        {
            object existing;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    // prevent a new set from spawning if it doesn't exist already.
                    if (!cacheKeyExists || !(existing is SortedSet<byte[]>))
                    {
                        return false;
                    }

                    // we must append to the existing set.
                    var newSet = existing as SortedSet<byte[]>;

                    if (newSet.Contains(value))
                    {
                        return false;
                    }

                    newSet.Add(value);
                    return true;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Removes any matching entries with the same value from an existing cached set.
        /// </summary>
        /// <typeparam name="T">The type of objects that are contained in the cached set.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry to locate and remove the value from.</param>
        /// <param name="value">The value to locate in the existing set and remove.</param>
        /// <returns>Returns false if the set doesn't exist as a cache entry or if the <paramref name="value"/> could not be found in the cached set. Otherwise true.</returns>
        public bool RemoveFromSet<T>(string cacheEntryKey, T value)
            where T : IConvertible
        {
            object existing;
            var removeCount = 0;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    // can't remove if the set doesn't exist already.
                    if (!cacheKeyExists || !(existing is SortedSet<string>))
                    {
                        return false;
                    }

                    var existingSet = existing as SortedSet<string>;
                    var targetValue = ValueToString(value);
                    removeCount = existingSet.RemoveWhere(x => string.Equals(x, targetValue, StringComparison.Ordinal));

                    return removeCount > 0;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Removes any matching byte array entries with the same value from an existing cached set.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry to locate and remove the value from.</param>
        /// <param name="value">The value to locate in the existing set and remove.</param>
        /// <returns>Returns false if the set doesn't exist as a cache entry or if the <paramref name="value"/> could not be found in the cached set. Otherwise true.</returns>
        public bool RemoveFromSet(string cacheEntryKey, byte[] value)
        {
            object existing;
            var removeCount = 0;

            try
            { 
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    // can't remove if the set doesn't exist already.
                    if (!cacheKeyExists || !(existing is SortedSet<byte[]>))
                    {
                        return false;
                    }

                    var existingSet = existing as SortedSet<byte[]>;
                    removeCount = existingSet.RemoveWhere(x => ByteArrayComparer.Default.Compare(x, value) == 0);

                    return removeCount > 0;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Fetches a stored set from the cache and returns it as a set. If the key was found then the set 
        /// is returned. If not found then a null is returned.
        /// </summary>
        /// <remarks>
        /// The returned set is implemented as a <see cref="SortedSet{T}"/>. And may differ from the order
        /// of items stored in the set in ASP.Net Cache engine.
        /// </remarks>
        /// <typeparam name="T">The type of objects that are contained in the cached set.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry to attempt to retrieve.</param>
        /// <returns>Returns the located set from the cache if the key was found. Otherwise null.</returns>
        public ISet<T> RetrieveSet<T>(string cacheEntryKey)
            where T : IConvertible
        {
            object existing;
            SortedSet<T> returnSet = null;

            try
            { 
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    // can't retrieve if the set doesn't exist already.
                    if (!cacheKeyExists || !(existing is SortedSet<string>))
                    {
                        return null;
                    }

                    returnSet = new SortedSet<T>(
                        ((SortedSet<string>)existing)
                            .Select(ParseValue<T>));

                    return returnSet;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Fetches a stored set from the cache and returns it as a set. If the key was found then the set 
        /// is returned. If not found then a null is returned.
        /// </summary>
        /// <remarks>
        /// The returned set is implemented as a <see cref="SortedSet{T}"/> of byte array values. And may 
        /// differ from the order of items stored in the set in ASP.Net Cache engine.
        /// </remarks>
        /// <param name="cacheEntryKey">The unique key of the cache entry to attempt to retrieve.</param>
        /// <returns>Returns the located set from the cache if the key was found. Otherwise null.</returns>
        public ISet<byte[]> RetrieveSet(string cacheEntryKey)
        {
            object existing;
            SortedSet<byte[]> returnSet = null;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    // can't retrieve if the set doesn't exist already.
                    if (!cacheKeyExists || !(existing is SortedSet<byte[]>))
                    {
                        return null;
                    }

                    returnSet = new SortedSet<byte[]>(
                        (existing as SortedSet<byte[]>).Select(x => x),
                        ByteArrayComparer.Default);

                    return returnSet;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Attempts to locate the <paramref name="value"/> in a cached set.
        /// </summary>
        /// <typeparam name="T">The type of objects that are contained in the cached set.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cached set to locate and within which to find the value.</param>
        /// <param name="value">The value to locate in the existing set.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the value is not present in the cached set.</returns>
        public bool ExistsInSet<T>(string cacheEntryKey, T value)
            where T : IConvertible
        {
            object existing;
            var anyMatched = false;

            try
            { 
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    // can't search the set if doesn't exist already.
                    if (!cacheKeyExists || !(existing is SortedSet<string>))
                    {
                        return false;
                    }

                    var existingSet = existing as SortedSet<string>;
                    var valueToFind = ValueToString(value);

                    anyMatched = existingSet.Contains(valueToFind, StringComparer.Ordinal);

                    return anyMatched;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Attempts to locate the <paramref name="value"/> in a cached set.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cached set to locate and within which to find the value.</param>
        /// <param name="value">The value to locate in the existing set.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the value is not present in the cached set.</returns>
        public bool ExistsInSet(string cacheEntryKey, byte[] value)
        {
            object existing;

            try
            {
                var anyMatched = false;

                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    // can't search the set if doesn't exist already.
                    if (!cacheKeyExists || !(existing is SortedSet<byte[]>))
                    {
                        return false;
                    }

                    var existingSet = existing as SortedSet<byte[]>;
                    anyMatched = existingSet.Contains(value, ByteArrayComparer.Default);

                    return anyMatched;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Gets the length of a set stored in the cache. If the key doesn't exist or isn't a set then returns null.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cached set to locate and for which the length must be read.</param>
        /// <returns>Returns the length of the set if found, or null if not found.</returns>
        public long? LengthOfSet(string cacheEntryKey)
        {
            object existing;
            long length = 0L;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheEntryExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    // can't search the set if doesn't exist already.
                    if (!cacheEntryExists)
                    {
                        return null;
                    }

                    if (existing is SortedSet<byte[]>)
                    {
                        length = ((SortedSet<byte[]>)existing).Count;
                        return length;
                    }

                    if (existing is SortedSet<string>)
                    {
                        length = ((SortedSet<string>)existing).Count;
                        return length;
                    }

                    return null;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        #endregion Set caching

        #region List caching

        /// <summary>
        /// Caches the given enumeration of <paramref name="listItems"/> values as a list in the cache.
        /// </summary>
        /// <typeparam name="T">The type of object being cached. All items of the list must be of this type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry to create.</param>
        /// <param name="listItems">The individual items to store as a list.</param>
        /// <param name="expiry">The time period that the data will be valid.</param>
        /// <returns>Returns true if the list was successfully created with all <paramref name="listItems"/> values cached.</returns>
        public bool CacheAsList<T>(string cacheEntryKey, IEnumerable<T> listItems, TimeSpan expiry)
            where T : IConvertible
        {
            object existing;

            try
            { 
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var existedPreviously = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    if (existedPreviously && !(existing is List<string>))
                    {
                        // cannot overwite another key as a list.
                        return false;
                    }

                    var newList = (existedPreviously && (existing is List<string>))
                        ? (List<string>)existing
                        : new List<string>();

                    newList.AddRange(listItems.Select(x => ValueToString(x)));

                    this._memoryCache.Set(cacheEntryKey, newList, expiry);
                    return true;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Caches the given enumeration of <paramref name="listItems"/> byte arrays as a list in the cache.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry to create.</param>
        /// <param name="listItems">The individual byte array values to store as a list.</param>
        /// <param name="expiry">The time period that the data will be valid.</param>
        /// <returns>Returns true if the list was successfully created with all <paramref name="listItems"/> values cached.</returns>
        public bool CacheAsList(string cacheEntryKey, IEnumerable<byte[]> listItems, TimeSpan expiry)
        {
            object existing;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var existedPreviously = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    if (existedPreviously && !(existing is List<byte[]>))
                    {
                        // cannot overwite another key as a list.
                        return false;
                    }

                    var newList = (existedPreviously && (existing is List<byte[]>))
                        ? (List<byte[]>)existing
                        : new List<byte[]>();

                    newList.AddRange(listItems);

                    this._memoryCache.Set(cacheEntryKey, newList, expiry);
                    return true;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Fetches a stored list from the cache and returns it as a <see cref="IList{T}"/>. If the key was 
        /// found then the list is returned. If not found then a null is returned.
        /// </summary>
        /// <typeparam name="T">The type of object that was cached in a list.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry to attempt to retrieve.</param>
        /// <returns>Returns the located list from the cache if the key was found. Otherwise null.</returns>
        public IList<T> RetrieveList<T>(string cacheEntryKey)
            where T : IConvertible
        {
            object existing;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    if (!cacheKeyExists || !(existing is List<string>))
                    {
                        // there is no cache entry or it is the wrong type.
                        return null;
                    }

                    var newList = new List<T>(
                        ((List<string>)existing).Select(ParseValue<T>));

                    return newList;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Fetches a stored list from the cache and returns it as a List of byte array values. If the key was 
        /// found then the list is returned. If not found then a null is returned.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry to attempt to retrieve.</param>
        /// <returns>Returns the located list from the cache if the key was found. Otherwise null.</returns>
        public IList<byte[]> RetrieveList(string cacheEntryKey)
        {
            object existing;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    if (!cacheKeyExists || !(existing is List<byte[]>))
                    {
                        // there is no cache entry or it is the wrong type.
                        return null;
                    }

                    var newList = new List<byte[]>((List<byte[]>)existing);
                    return newList;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Appends a new value to the end of an existing cached list.
        /// </summary>
        /// <typeparam name="T">The type of object being appended to the cached list. All items of the list must be of the same type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the list that the 
        /// <paramref name="value"/> will be appended to.</param>
        /// <param name="value">The value to append to the cached list.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the value cannot be appended. Otherwise true.</returns>
        public bool AppendToList<T>(string cacheEntryKey, T value)
            where T : IConvertible
        {
            object existing;
            var newValue = ValueToString(value);

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    if (!cacheKeyExists || !(existing is List<string>))
                    {
                        // there is no cache entry or it is the wrong type.
                        return false;
                    }

                    ((List<string>)existing).Add(newValue);

                    return true;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Appends a new byte array value to the end of an existing cached list.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the list that the 
        /// <paramref name="value"/> will be appended to.</param>
        /// <param name="value">The value to append to the cached list.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the value cannot be appended. Otherwise true.</returns>
        public bool AppendToList(string cacheEntryKey, byte[] value)
        {
            object existing;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    if (!cacheKeyExists || !(existing is List<byte[]>))
                    {
                        // there is no cache entry or it is the wrong type.
                        return false;
                    }

                    ((List<byte[]>)existing).Add(value);

                    return true;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Truncates values from the cached list so that only the values in the range specified remain.
        /// </summary>
        /// <example>
        /// <para>To remove the first two entries, specify <paramref name="firstIndexKept"/>=2 and <paramref name="lastIndexKept"/>=-1.</para>
        /// <para>To remove the last five entries, specify <paramref name="firstIndexKept"/>=0 and <paramref name="lastIndexKept"/>=-6.</para>
        /// <para>To remove the first and last entries, specify <paramref name="firstIndexKept"/>=1 and <paramref name="lastIndexKept"/>=-2.</para>
        /// </example>
        /// <param name="cacheEntryKey">The unique key of the cached list to attempt to shrink.</param>
        /// <param name="firstIndexKept">The zero-based value of the first value from the list that must be kept. Negative 
        /// values refer to the position from the end of the list (i.e. -1 is the last list entry and -2 is the second last entry).</param>
        /// <param name="lastIndexKept">The zero-based value of the last value from the list that must be kept. Negative 
        /// values refer to the position from the end of the list (i.e. -1 is the last list entry and -2 is the second last entry).</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the list cannot be shrunk. Otherwise true.</returns>
        public bool ShrinkList(string cacheEntryKey, long firstIndexKept, long lastIndexKept)
        {
            object existing;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    if (!cacheKeyExists || (!(existing is List<string>) && !(existing is List<byte[]>)))
                    {
                        // key doesn't exist as a cached list
                        return false;
                    }

                    if (existing is List<byte[]>)
                    {
                        var retrievedList = (List<byte[]>)existing;
                        long length = retrievedList.Count;
                        if (firstIndexKept < 0L)
                        {
                            // get the real index based on this end reference
                            firstIndexKept = length + firstIndexKept;
                        }

                        if (lastIndexKept < 0L)
                        {
                            // get the real index based on this end reference
                            lastIndexKept = length + lastIndexKept;
                        }

                        // if the caller entered criteria that would result in an invalid range then rather clear
                        if (lastIndexKept < firstIndexKept)
                        {
                            retrievedList.Clear();
                            return true;
                        }

                        for (var i = (int)(length - 1); i >= 0; i--)
                        {
                            if (i > lastIndexKept)
                            {
                                retrievedList.RemoveAt(i);
                            }
                            else if (i < firstIndexKept)
                            {
                                retrievedList.RemoveAt(i);
                            }
                        }

                        return true;
                    }

                    if (existing is List<string>)
                    {
                        var retrievedList = (List<string>)existing;
                        long length = retrievedList.Count;
                        if (firstIndexKept < 0L)
                        {
                            // get the real index based on this end reference
                            firstIndexKept = length + firstIndexKept;
                        }

                        if (lastIndexKept < 0L)
                        {
                            // get the real index based on this end reference
                            lastIndexKept = length + lastIndexKept;
                        }

                        // if the caller entered criteria that would result in an invalid range then rather clear
                        if (lastIndexKept < firstIndexKept)
                        {
                            retrievedList.Clear();
                            return true;
                        }

                        for (var i = (int)(length - 1); i >= 0; i--)
                        {
                            if (i > lastIndexKept)
                            {
                                retrievedList.RemoveAt(i);
                            }
                            else if (i < firstIndexKept)
                            {
                                retrievedList.RemoveAt(i);
                            }
                        }

                        return true;
                    }
                }

                return false;
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Removes any matching entries with the same value from an existing cached list.
        /// </summary>
        /// <typeparam name="T">The type of objects that are contained in the cached list.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cached list to locate and remove the value from.</param>
        /// <param name="value">The value to locate in the existing list and remove.</param>
        /// <returns>Returns -1 list doesn't exist as a cache entry or if the <paramref name="value"/> could not be found in the cached list. Otherwise returns the number of removed items.</returns>
        public long RemoveFromList<T>(string cacheEntryKey, T value)
        {
            object existing;

            try
            { 
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    if (!cacheKeyExists || !(existing is List<string>))
                    {
                        // key doesn't exist as a cached list
                        return -1L;
                    }

                    var searchValue = ValueToString(value);
                    return ((List<string>)existing).RemoveAll(x => string.Equals(x, searchValue, StringComparison.Ordinal));
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Removes any matching entries with the same byte array value from an existing cached list.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cached list to locate and remove the value from.</param>
        /// <param name="value">The value to locate in the existing list and remove.</param>
        /// <returns>Returns -1 list doesn't exist as a cache entry or if the <paramref name="value"/> could not be found in the cached list. Otherwise returns the number of removed items.</returns>
        public long RemoveFromList(string cacheEntryKey, byte[] value)
        {
            object existing;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    if (!cacheKeyExists || !(existing is List<byte[]>))
                    {
                        // key doesn't exist as a cached list
                        return -1L;
                    }

                    return ((List<byte[]>)existing)
                        .RemoveAll(x => ByteArrayComparer.Default.Equals(x, value));
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Removes all items from an existing cached list.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the list that must be cleared.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the list cannot be cleared. Otherwise true.</returns>
        public bool ClearList(string cacheEntryKey)
        {
            // for clearing we can use the shrink method to shrink to an empty length.
            return this.ShrinkList(cacheEntryKey, -1L, 0L);
        }

        /// <summary>
        /// Gets the length of a list stored in the cache. If the key doesn't exist or isn't a list then returns null.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cached list to locate and for which the length must be read.</param>
        /// <returns>Returns the length of the list if found, or null if not found.</returns>
        public long? LengthOfList(string cacheEntryKey)
        {
            object existing;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    if (!cacheKeyExists || (!(existing is List<string>) && !(existing is List<byte[]>)))
                    {
                        // key doesn't exist as a cached list
                        return null;
                    }

                    if (existing is List<byte[]>)
                    {
                        var retrievedList = (List<byte[]>)existing;
                        var length = retrievedList.Count;
                        return length;
                    }

                    if (existing is List<string>)
                    {
                        var retrievedList = (List<string>)existing;
                        var length = retrievedList.Count;
                        return length;
                    }
                }

                return null;
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        #endregion List caching

        #region Dictionary caching

        /// <summary>
        /// Caches the given dictionary of items as a new dictionary in the cache engine.
        /// </summary>
        /// <typeparam name="T">The type of object being cached. All values of the dictionary must be of this type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry which will contain the dictionary.</param>
        /// <param name="dictionaryItems">The items to cache as a dictionary.</param>
        /// <param name="expiry">The time period that the data will be valid.</param>
        /// <returns>Returns true if the dictionary was successfully created with all <paramref name="dictionaryItems"/> values cached.</returns>
        public bool CacheAsDictionary<T>(string cacheEntryKey, IDictionary<string, T> dictionaryItems, TimeSpan expiry)
            where T : IConvertible
        {
            object existing;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var existedPreviously = this._memoryCache.TryGetValue(cacheEntryKey, out existing);
                    ConcurrentDictionary<string, string> dictionary = null;

                    if (existedPreviously && !(existing is Dictionary<string, string>))
                    {
                        // cannot overwite another key as a list.
                        return false;
                    }
                    else if (existing is ConcurrentDictionary<string, string>)
                    {
                        dictionary = (ConcurrentDictionary<string, string>)existing;
                    }
                    else
                    {
                        dictionary = new ConcurrentDictionary<string, string>();
                    }

                    foreach (var item in dictionaryItems)
                    {
                        var itemValue = ValueToString(item.Value);
                        dictionary.AddOrUpdate(item.Key, itemValue, (k, v) => itemValue);
                    }

                    this._memoryCache.Set(cacheEntryKey, dictionary, expiry);
                    return true;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Caches the given dictionary of byte array items as a new dictionary in the cache engine.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which will contain the dictionary.</param>
        /// <param name="dictionaryItems">The items to cache as a dictionary.</param>
        /// <param name="expiry">The time period that the data will be valid.</param>
        /// <returns>Returns true if the dictionary was successfully created with all <paramref name="dictionaryItems"/> byte array values cached.</returns>
        public bool CacheAsDictionary(string cacheEntryKey, IDictionary<string, byte[]> dictionaryItems, TimeSpan expiry)
        {
            object existing;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var existedPreviously = this._memoryCache.TryGetValue(cacheEntryKey, out existing);
                    ConcurrentDictionary<string, byte[]> dictionary = null;

                    if (existedPreviously && !(existing is Dictionary<string, byte[]>))
                    {
                        // cannot overwite another key as a list.
                        return false;
                    }
                    else if (existing is ConcurrentDictionary<string, byte[]>)
                    {
                        dictionary = (ConcurrentDictionary<string, byte[]>)existing;
                    }
                    else
                    {
                        dictionary = new ConcurrentDictionary<string, byte[]>();
                    }

                    foreach (var item in dictionaryItems)
                    {
                        dictionary.AddOrUpdate(item.Key, item.Value, (k, v) => item.Value);
                    }

                    this._memoryCache.Set(cacheEntryKey, dictionary, expiry);
                    return true;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Adds a new dictionary entry for the given value into an existing cached dictionary with 
        /// the <paramref name="dictionaryKey"/> specified.
        /// </summary>
        /// <typeparam name="T">The type of object being added to the cached dictionary. All values of the 
        /// dictionary values must be of the same type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary that the 
        /// <paramref name="value"/> will be added to.</param>
        /// <param name="dictionaryKey">The unique name within the dictionary for the value being added.</param>
        /// <param name="value">The value to add into the cached dictionary.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the value cannot be added. Otherwise true.</returns>
        public bool AddToDictionary<T>(string cacheEntryKey, string dictionaryKey, T value)
            where T : IConvertible
        {
            object existing;

            try
            {
                var cacheKeyExists = false;

                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);
                }

                if (!cacheKeyExists || !(existing is ConcurrentDictionary<string, string>))
                {
                    // there is no cache entry or it is the wrong type.
                    return false;
                }

                return ((ConcurrentDictionary<string, string>)existing).TryAdd(dictionaryKey, ValueToString(value));
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Adds a new dictionary entry for the given byte array value into an existing cached dictionary with 
        /// the <paramref name="dictionaryKey"/> specified.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary that the 
        /// <paramref name="value"/> will be added to.</param>
        /// <param name="dictionaryKey">The unique name within the dictionary for the value being added.</param>
        /// <param name="value">The byte array value to add into the cached dictionary.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the value cannot be added. Otherwise true.</returns>
        public bool AddToDictionary(string cacheEntryKey, string dictionaryKey, byte[] value)
        {
            object existing;

            try
            {
                var cacheKeyExists = false;

                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);
                }

                if (!cacheKeyExists || !(existing is ConcurrentDictionary<string, byte[]>))
                {
                    // there is no cache entry or it is the wrong type.
                    return false;
                }

                return ((ConcurrentDictionary<string, byte[]>)existing).TryAdd(dictionaryKey, value);
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Updates an existing dictionary entry with the given value in an existing cached dictionary for 
        /// the <paramref name="dictionaryKey"/> specified.
        /// </summary>
        /// <typeparam name="T">The type of object being updated in the cached dictionary. All values of the 
        /// dictionary values must be of the same type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary.</param>
        /// <param name="dictionaryKey">The unique name within the dictionary for the value being updated.</param>
        /// <param name="value">The value to update in the cached dictionary.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the <paramref name="dictionaryKey"/> cannot
        /// be found or the value cannot be updated. Otherwise true.</returns>
        public bool UpdateDictionaryEntry<T>(string cacheEntryKey, string dictionaryKey, T value)
            where T : IConvertible
        {
            object existing;

            try
            {
                var cacheKeyExists = false;

                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);
                }

                if (!cacheKeyExists || !(existing is ConcurrentDictionary<string, string>))
                {
                    // there is no cache entry or it is the wrong type.
                    return false;
                }

                if (!((ConcurrentDictionary<string, string>)existing).ContainsKey(dictionaryKey))
                {
                    return false;
                }

                ((ConcurrentDictionary<string, string>)existing)[dictionaryKey] = ValueToString(value);
                return true;
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Updates an existing dictionary entry with the given byte array value in an existing cached 
        /// dictionary for the <paramref name="dictionaryKey"/> specified.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary.</param>
        /// <param name="dictionaryKey">The unique name within the dictionary for the value being updated.</param>
        /// <param name="value">The value to update in the cached dictionary.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the <paramref name="dictionaryKey"/> cannot
        /// be found or the value cannot be updated. Otherwise true.</returns>
        public bool UpdateDictionaryEntry(string cacheEntryKey, string dictionaryKey, byte[] value)
        {
            object existing;

            try
            {
                var cacheKeyExists = false;

                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);
                }

                if (!cacheKeyExists || !(existing is ConcurrentDictionary<string, byte[]>))
                {
                    // there is no cache entry or it is the wrong type.
                    return false;
                }

                if (!((ConcurrentDictionary<string, byte[]>)existing).ContainsKey(dictionaryKey))
                {
                    return false;
                }

                ((ConcurrentDictionary<string, byte[]>)existing)[dictionaryKey] = value;
                return true;
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Removes a dictionary entry from an existing cached dictionary for the <paramref name="dictionaryKey"/> specified.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary.</param>
        /// <param name="dictionaryKey">The unique name within the dictionary for the value being removed.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the <paramref name="dictionaryKey"/> cannot
        /// be removed. Otherwise true.</returns>
        public bool RemoveFromDictionary(string cacheEntryKey, string dictionaryKey)
        {
            object existing;

            try
            {
                var cacheKeyExists = false;

                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);
                }

                if (!cacheKeyExists || (!(existing is ConcurrentDictionary<string, string>) && !(existing is ConcurrentDictionary<string, byte[]>)))
                {
                    // there is no cache entry or it is the wrong type.
                    return false;
                }

                if (cacheKeyExists && (existing is ConcurrentDictionary<string, string>))
                {
                    string unusedValue;
                    return ((ConcurrentDictionary<string, string>)existing).TryRemove(dictionaryKey, out unusedValue);
                }

                if (cacheKeyExists && (existing is ConcurrentDictionary<string, byte[]>))
                {
                    byte[] unusedValue;
                    return ((ConcurrentDictionary<string, byte[]>)existing).TryRemove(dictionaryKey, out unusedValue);
                }

                return false;
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Retrieves all entries in a cached dictionary as a new <see cref="IDictionary{TKey, TValue}"/>. 
        /// </summary>
        /// <typeparam name="T">The type of object which was written in the cached dictionary. All values of the 
        /// dictionary values must be of the same type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary.</param>
        /// <returns>Returns the located dictionary from the cache if the key was found. Otherwise null.</returns>
        public IDictionary<string, T> RetrieveDictionary<T>(string cacheEntryKey)
            where T : IConvertible
        {
            object existing;

            try
            {
                var cacheKeyExists = false;

                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);
                }

                if (!cacheKeyExists || !(existing is ConcurrentDictionary<string, string>))
                {
                    // there is no cache entry or it is the wrong type.
                    return null;
                }

                if (cacheKeyExists && (existing is ConcurrentDictionary<string, string>))
                {
                    var newDictionary =
                        ((ConcurrentDictionary<string, string>)existing)
                            .ToDictionary(x => x.Key, x => ParseValue<T>(x.Value));

                    return newDictionary;
                }

                return null;
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Retrieves all entries in a cached dictionary as a new <see cref="IDictionary{TKey, TValue}"/>. 
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary.</param>
        /// <returns>Returns the located dictionary containing byte array values from the cache if the key was found. Otherwise null.</returns>
        public IDictionary<string, byte[]> RetrieveDictionary(string cacheEntryKey)
        {
            object existing;

            try
            {
                var cacheKeyExists = false;

                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);
                }

                if (!cacheKeyExists || !(existing is ConcurrentDictionary<string, byte[]>))
                {
                    // there is no cache entry or it is the wrong type.
                    return null;
                }

                if (cacheKeyExists && (existing is ConcurrentDictionary<string, byte[]>))
                {
                    var newDictionary =
                        ((ConcurrentDictionary<string, byte[]>)existing)
                            .ToDictionary(x => x.Key, x => x.Value);

                    return newDictionary;
                }

                return null;
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Retrieves a single entry from a cached dictionary located by the <paramref name="dictionaryKey"/>. 
        /// </summary>
        /// <typeparam name="T">The type of object which was written in the cached dictionary. All values of the 
        /// dictionary values must be of the same type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary.</param>
        /// <param name="dictionaryKey">The unique name within the dictionary for the value being sought.</param>
        /// <param name="value">Returns the value found in the dictionary cache. If not found the default value is returned.</param>
        /// <returns>Returns true if the value was located in the cached dictionary. Otherwise false.</returns>
        public bool RetrieveDictionaryEntry<T>(string cacheEntryKey, string dictionaryKey, out T value)
            where T : IConvertible
        {
            object existing;

            try
            {
                var cacheKeyExists = false;

                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);
                }

                if (!cacheKeyExists || !(existing is ConcurrentDictionary<string, string>))
                {
                    // there is no cache entry or it is the wrong type.
                    value = default(T);
                    return false;
                }

                string retrieved;
                var success = ((ConcurrentDictionary<string, string>)existing).TryGetValue(dictionaryKey, out retrieved);

                if (success)
                {
                    value = ParseValue<T>(retrieved);
                    return true;
                }

                value = default(T);
                return false;
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Retrieves a single entry (a byte array) from a cached dictionary located by the <paramref name="dictionaryKey"/>. 
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary.</param>
        /// <param name="dictionaryKey">The unique name within the dictionary for the value being sought.</param>
        /// <param name="value">Returns the byte array value found in the dictionary cache. If not found then null is returned.</param>
        /// <returns>Returns true if the value was located in the cached dictionary. Otherwise false.</returns>
        public bool RetrieveDictionaryEntry(string cacheEntryKey, string dictionaryKey, out byte[] value)
        {
            object existing;

            try
            {
                var cacheKeyExists = false;

                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);
                }

                if (!cacheKeyExists || !(existing is ConcurrentDictionary<string, byte[]>))
                {
                    // there is no cache entry or it is the wrong type.
                    value = null;
                    return false;
                }

                return ((ConcurrentDictionary<string, byte[]>)existing).TryGetValue(dictionaryKey, out value);
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Attempts to locate the <paramref name="dictionaryKey"/> in a cached dictionary.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the dictionary.</param>
        /// <param name="dictionaryKey">The unique name within the dictionary for the value being sought.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the key is not present in the cached dictionary.</returns>
        public bool ExistsInDictionary(string cacheEntryKey, string dictionaryKey)
        {
            object existing;

            try
            {
                var cacheKeyExists = false;

                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);
                }

                if (!cacheKeyExists || (!(existing is ConcurrentDictionary<string, byte[]>) && !(existing is ConcurrentDictionary<string, string>)))
                {
                    // there is no cache entry or it's the wrong type.
                    return false;
                }

                if (existing is ConcurrentDictionary<string, byte[]>)
                {
                    return ((ConcurrentDictionary<string, byte[]>)existing).ContainsKey(dictionaryKey);
                }

                if (existing is ConcurrentDictionary<string, string>)
                {
                    return ((ConcurrentDictionary<string, string>)existing).ContainsKey(dictionaryKey);
                }

                return false;
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Gets the length of a dictionary stored in the cache. If the key doesn't exist or isn't a dictionary then returns null.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cached dictionary to locate and for which the length must be read.</param>
        /// <returns>Returns the length of the dictionary if found, or null if not found.</returns>
        public long? LengthOfDictionary(string cacheEntryKey)
        {
            object existing;

            try
            {
                var cacheKeyExists = false;

                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);
                }

                if (!cacheKeyExists)
                {
                    // there is no cache entry or it is the wrong type.
                    return null;
                }

                if (existing is ConcurrentDictionary<string, byte[]>)
                {
                    return ((ConcurrentDictionary<string, byte[]>)existing).Count;
                }

                if (existing is ConcurrentDictionary<string, string>)
                {
                    return ((ConcurrentDictionary<string, string>)existing).Count;
                }

                return null;
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        #endregion Dictionary caching

        #region Queue caching

        /// <summary>
        /// Caches the given enumeration of <paramref name="queuedItems"/> values as a queue in the cache.
        /// </summary>
        /// <typeparam name="T">The type of object being cached. All items of the queue must be of this type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry to create.</param>
        /// <param name="queuedItems">The individual items to store as a queue.</param>
        /// <param name="expiry">The time period that the data will be valid. May be set to never expire by setting <see cref="TimeSpan.MaxValue"/>.</param>
        /// <returns>Returns true if the queue was successfully created with all <paramref name="queuedItems"/> values cached.</returns>
        public bool CacheAsQueue<T>(string cacheEntryKey, IEnumerable<T> queuedItems, TimeSpan expiry)
            where T : IConvertible
        {
            ConcurrentQueue<string> queue = null;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    object existing;
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var existedPreviously = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    if (existedPreviously && !(existing is ConcurrentQueue<string>))
                    {
                        // cannot overwite another key as a queue.
                        return false;
                    }
                    else if (existedPreviously && existing is ConcurrentQueue<string>)
                    {
                        queue = (ConcurrentQueue<string>)existing;
                    }
                    else
                    {
                        queue = new ConcurrentQueue<string>();
                        this._memoryCache.Set(cacheEntryKey, queue, expiry);
                    }
                }

                foreach (var item in queuedItems)
                {
                    queue.Enqueue(ValueToString(item));
                }
            }
            finally
            {
                this.CleanOldLocks();
            }

            return true;
        }

        /// <summary>
        /// Caches the given enumeration of <paramref name="queuedItems"/> byte arrays as a queue in the cache.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry to create.</param>
        /// <param name="queuedItems">The individual byte array values to store as a queue.</param>
        /// <param name="expiry">The time period that the data will be valid. May be set to never expire by setting <see cref="TimeSpan.MaxValue"/>.</param>
        /// <returns>Returns true if the queue was successfully created with all <paramref name="queuedItems"/> values cached.</returns>
        public bool CacheAsQueue(string cacheEntryKey, IEnumerable<byte[]> queuedItems, TimeSpan expiry)
        {
            ConcurrentQueue<byte[]> queue = null;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    object existing;
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var existedPreviously = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    if (existedPreviously && !(existing is ConcurrentQueue<byte[]>))
                    {
                        // cannot overwite another key as a queue.
                        return false;
                    }
                    else if (existedPreviously && existing is ConcurrentQueue<byte[]>)
                    {
                        queue = (ConcurrentQueue<byte[]>)existing;
                    }
                    else
                    {
                        queue = new ConcurrentQueue<byte[]>();

                        this._memoryCache.Set(cacheEntryKey, queue, expiry);
                    }
                }

                foreach (var item in queuedItems)
                {
                    queue.Enqueue(item);
                }
            }
            finally
            {
                this.CleanOldLocks();
            }

            return true;
        }

        /// <summary>
        /// Pushes a new value to the end of an existing cached queue.
        /// </summary>
        /// <typeparam name="T">The type of object being pushed to the cached queue. All items of the queue must be of the same type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the queue.</param>
        /// <param name="value">The value to append to the cached queue.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the value cannot be pushed to the queue. Otherwise true.</returns>
        public bool PushQueue<T>(string cacheEntryKey, T value)
            where T : IConvertible
        {
            object existing;

            try
            {
                var cacheKeyExists = false;
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);
                }

                if (!cacheKeyExists || !(existing is ConcurrentQueue<string>))
                {
                    // key doesn't exist as a cached queue
                    return false;
                }

                ((ConcurrentQueue<string>)existing).Enqueue(ValueToString(value));
                return true;
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Pushes a new byte array to the end of an existing cached queue.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the queue.</param>
        /// <param name="value">The value to append to the cached queue.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the value cannot be pushed to the queue. Otherwise true.</returns>
        public bool PushQueue(string cacheEntryKey, byte[] value)
        {
            object existing;

            try
            {
                var cacheKeyExists = false;
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);
                }

                if (!cacheKeyExists || !(existing is ConcurrentQueue<byte[]>))
                {
                    // key doesn't exist as a cached queue
                    return false;
                }

                ((ConcurrentQueue<byte[]>)existing).Enqueue(value);
                return true;
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Pops the next value in the cached queue and returns the value.
        /// </summary>
        /// <typeparam name="T">The type of the objects stored in the cached queue. All items of the queue must be of the same type.</typeparam>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the queue.</param>
        /// <param name="value">Returns the next value from the cached queue. If not found then a default value is returned.</param>
        /// <returns>Returns true if the next value in the cached queue was successfully returned in <paramref name="value"/>. Otherwise false.</returns>
        public bool PopQueue<T>(string cacheEntryKey, out T value)
            where T : IConvertible
        {
            object existing;

            try
            {
                var cacheKeyExists = false;
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);
                }

                if (!cacheKeyExists || !(existing is ConcurrentQueue<string>))
                {
                    // key doesn't exist as a cached queue
                    value = default(T);
                    return false;
                }

                string queueValue;
                var success = ((ConcurrentQueue<string>)existing).TryDequeue(out queueValue);
                
                value = success
                    ? ParseValue<T>(queueValue)
                    : default(T);

                return success;
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Pops the next byte array in the cached queue and returns the value.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the queue.</param>
        /// <param name="value">Returns the next byte array value from the cached queue. If not found then null is returned.</param>
        /// <returns>Returns true if the next value in the cached queue was successfully returned in <paramref name="value"/>. Otherwise false.</returns>
        public bool PopQueue(string cacheEntryKey, out byte[] value)
        {
            object existing;

            try
            {
                var cacheKeyExists = false;
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);
                }

                if (!cacheKeyExists || !(existing is ConcurrentQueue<byte[]>))
                {
                    // key doesn't exist as a cached queue
                    value = null;
                    return false;
                }

                byte[] queueValue;
                var success = ((ConcurrentQueue<byte[]>)existing).TryDequeue(out queueValue);

                value = success
                    ? queueValue
                    : null;

                return success;
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Removes all items from an existing cached queue.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cache entry which contains the queue that must be cleared.</param>
        /// <returns>Returns false if the cache entry doesn't exist or if the queue cannot be cleared. Otherwise true.</returns>
        public bool ClearQueue(string cacheEntryKey)
        {
            object existing;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    if (!cacheKeyExists || (!(existing is ConcurrentQueue<string>) && !(existing is ConcurrentQueue<byte[]>)))
                    {
                        // key doesn't exist as a cached list
                        return false;
                    }

                    if (existing is ConcurrentQueue<string>)
                    {
                        string value;
                        while (((ConcurrentQueue<string>)existing).TryDequeue(out value))
                        {
                            // continue to dequeue until empty - the lock will prevent new additions.
                        }

                        return true;
                    }

                    if (existing is ConcurrentQueue<byte[]>)
                    {
                        byte[] value;
                        while (((ConcurrentQueue<byte[]>)existing).TryDequeue(out value))
                        {
                            // continue to dequeue until empty - the lock will prevent new additions.
                        }

                        return true;
                    }

                    return false;
                }
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        /// <summary>
        /// Gets the length of a queue stored in the cache. If the key doesn't exist or isn't a queue then returns null.
        /// </summary>
        /// <param name="cacheEntryKey">The unique key of the cached queue to locate and for which the length must be read.</param>
        /// <returns>Returns the length of the queue if found, or null if not found.</returns>
        public long? LengthOfQueue(string cacheEntryKey)
        {
            object existing;

            try
            {
                lock (this._activeLocks.GetOrAdd(cacheEntryKey, _ => new object()))
                {
                    this._lockObjectAge.AddOrUpdate(cacheEntryKey, DateTime.UtcNow, (k, v) => DateTime.UtcNow);
                    var cacheKeyExists = this._memoryCache.TryGetValue(cacheEntryKey, out existing);

                    if (!cacheKeyExists || (!(existing is ConcurrentQueue<string>) && !(existing is ConcurrentQueue<byte[]>)))
                    {
                        // key doesn't exist as a cached list
                        return null;
                    }

                    if (existing is ConcurrentQueue<byte[]>)
                    {
                        var retrievedQueue = (ConcurrentQueue<byte[]>)existing;
                        return retrievedQueue.Count;
                    }

                    if (existing is ConcurrentQueue<string>)
                    {
                        var retrievedQueue = (ConcurrentQueue<string>)existing;
                        return retrievedQueue.Count;
                    }
                }

                return null;
            }
            finally
            {
                this.CleanOldLocks();
            }
        }

        #endregion Queue caching

        #region Private methods

        /// <summary>
        /// Parses the string stored in ASP.Net Cache engine as the type <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type to which the value must be converted.</typeparam>
        /// <param name="value">The stored ASP.Net Cache engine value to be converted.</param>
        /// <returns>Returns the typed value parsed from the ASP.Net Cache engine value.</returns>
        private static T ParseValue<T>(object value)
            where T : IConvertible
        {
            return (T)Convert.ChangeType(value.ToString(), typeof(T), CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Converts an object to a string for storing in cache or for comparison to stored strings. 
        /// </summary>
        /// <param name="value">The value to be converted to a string.</param>
        /// <returns>Returns the value in a string formatted string which is round-trip-aware.</returns>
        private static string ValueToString(object value)
        {
            if (value is DateTime)
            {
                // the round-trip format for date time values depends on whether or not there is a time
                // component.  Without time, the format is "yyyy-MM-dd" but with time it is
                // "yyyy-MM-ddTHH:mm:ss.fffffff"
                var dateTimeValue = (DateTime)value;

                return (dateTimeValue.TimeOfDay.Ticks > 0L)
                    ? dateTimeValue.ToString("o", DateTimeFormatInfo.InvariantInfo)
                    : dateTimeValue.ToString("yyyy-MM-dd", DateTimeFormatInfo.InvariantInfo);
            }
            else if (value is double)
            {
                return ((double)value).ToString("r", NumberFormatInfo.InvariantInfo);
            }
            else if (value is float)
            {
                return ((float)value).ToString("r", NumberFormatInfo.InvariantInfo);
            }

            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Clean-up the locks which haven't been used in 10 minutes.
        /// </summary>
        private void CleanOldLocks()
        {
            lock (this._lockObjectAge)
            {
                foreach (var key in this._lockObjectAge.Keys)
                {
                    if (this._lockObjectAge[key] < DateTime.UtcNow.AddMinutes(-10d))
                    {
                        object unusedLock;
                        DateTime unusedDate;
                        this._activeLocks.TryRemove(key, out unusedLock);
                        this._lockObjectAge.TryRemove(key, out unusedDate);
                    }
                }
            }
        }

        #endregion Private methods
    }
}
